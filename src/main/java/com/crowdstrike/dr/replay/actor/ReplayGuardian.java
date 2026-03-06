package com.crowdstrike.dr.replay.actor;

import com.crowdstrike.dr.replay.config.ReplayConfig;
import com.crowdstrike.dr.replay.destination.KafkaDestination;
import com.crowdstrike.dr.replay.destination.ReplayDestination;
import com.crowdstrike.dr.replay.destination.RestDestination;
import com.crowdstrike.dr.replay.iceberg.IcebergCatalogFactory;
import com.crowdstrike.dr.replay.model.ReplayJob;
import com.crowdstrike.dr.replay.model.ReplayJobRequest;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Guardian actor: creates replay jobs, tracks them, and forwards commands to job actors.
 */
public final class ReplayGuardian extends AbstractBehavior<ReplayGuardianCommand> {

    private final ActorContext<ReplayGuardianCommand> ctx;
    private final ReplayConfig config;
    private final Catalog catalog;
    private final Map<String, ReplayJob> jobs = new ConcurrentHashMap<>();
    private final Map<String, org.apache.pekko.actor.typed.ActorRef<ReplayJobCommand>> jobActors = new ConcurrentHashMap<>();

    private ReplayGuardian(
            ActorContext<ReplayGuardianCommand> context,
            ReplayConfig config) {
        super(context);
        this.ctx = context;
        this.config = config;
        this.catalog = IcebergCatalogFactory.createHadoopCatalog(config.getIcebergWarehousePath());
    }

    public static Behavior<ReplayGuardianCommand> create(ReplayConfig config) {
        return Behaviors.setup(ctx -> new ReplayGuardian(ctx, config));
    }

    @Override
    public Receive<ReplayGuardianCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReplayGuardianCommand.CreateJob.class, this::onCreateJob)
                .onMessage(ReplayGuardianCommand.ListJobs.class, this::onListJobs)
                .onMessage(ReplayGuardianCommand.GetJob.class, this::onGetJob)
                .onMessage(ReplayGuardianCommand.StartJob.class, this::onStartJob)
                .onMessage(ReplayGuardianCommand.PauseJob.class, this::onPauseJob)
                .onMessage(ReplayGuardianCommand.ResumeJob.class, this::onResumeJob)
                .onMessage(ReplayGuardianCommand.CancelJob.class, this::onCancelJob)
                .build();
    }

    private Behavior<ReplayGuardianCommand> onCreateJob(ReplayGuardianCommand.CreateJob msg) {
        ReplayJobRequest req = msg.request();
        String id = UUID.randomUUID().toString();
        ReplayJob.DestinationType destType = "REST".equalsIgnoreCase(req.getDestinationType())
                ? ReplayJob.DestinationType.REST
                : ReplayJob.DestinationType.KAFKA;
        double speed = req.getSpeedMultiplier() > 0 ? req.getSpeedMultiplier() : 1.0;

        ReplayJob job = new ReplayJob(
                id,
                req.getTablePath(),
                destType,
                req.getDestinationConfig() != null ? req.getDestinationConfig() : "",
                speed,
                req.getStartEventTime(),
                req.getEndEventTime()
        );

        try {
            TableIdentifier tableId = parseTableId(req.getTablePath());
            var table = catalog.loadTable(tableId);
            ReplayDestination destination = createDestination(destType, req.getDestinationConfig());
            org.apache.pekko.actor.typed.ActorRef<ReplayJobCommand> ref =
                    ctx.spawn(ReplayJobActor.create(job, table, destination), "job-" + id);
            jobs.put(id, job);
            jobActors.put(id, ref);
            msg.replyTo().tell(job);
        } catch (Exception e) {
            job.setStatus(ReplayJob.Status.FAILED);
            job.setErrorMessage(e.getMessage());
            jobs.put(id, job);
            msg.replyTo().tell(job);
        }
        return this;
    }

    private Behavior<ReplayGuardianCommand> onListJobs(ReplayGuardianCommand.ListJobs msg) {
        msg.replyTo().tell(new ArrayList<>(jobs.values()));
        return this;
    }

    private Behavior<ReplayGuardianCommand> onGetJob(ReplayGuardianCommand.GetJob msg) {
        var ref = jobActors.get(msg.jobId());
        if (ref != null) {
            ref.tell(new ReplayJobCommand.GetStatus(msg.replyTo()));
        } else {
            ReplayJob j = jobs.get(msg.jobId());
            msg.replyTo().tell(j);
        }
        return this;
    }

    private Behavior<ReplayGuardianCommand> onStartJob(ReplayGuardianCommand.StartJob msg) {
        var ref = jobActors.get(msg.jobId());
        boolean ok = ref != null;
        if (ok) {
            ref.tell(new ReplayJobCommand.Start());
        }
        msg.replyTo().tell(ok);
        return this;
    }

    private Behavior<ReplayGuardianCommand> onPauseJob(ReplayGuardianCommand.PauseJob msg) {
        var ref = jobActors.get(msg.jobId());
        boolean ok = ref != null;
        if (ok) ref.tell(new ReplayJobCommand.Pause());
        msg.replyTo().tell(ok);
        return this;
    }

    private Behavior<ReplayGuardianCommand> onResumeJob(ReplayGuardianCommand.ResumeJob msg) {
        var ref = jobActors.get(msg.jobId());
        boolean ok = ref != null;
        if (ok) ref.tell(new ReplayJobCommand.Resume());
        msg.replyTo().tell(ok);
        return this;
    }

    private Behavior<ReplayGuardianCommand> onCancelJob(ReplayGuardianCommand.CancelJob msg) {
        var ref = jobActors.get(msg.jobId());
        boolean ok = ref != null;
        if (ok) ref.tell(new ReplayJobCommand.Cancel());
        msg.replyTo().tell(ok);
        return this;
    }

    private ReplayDestination createDestination(ReplayJob.DestinationType type, String destConfig) {
        if (type == ReplayJob.DestinationType.KAFKA) {
            String topic = destConfig != null && !destConfig.isBlank() ? destConfig : "replay-events";
            return new KafkaDestination(config.getKafkaBootstrapServers(), topic, null);
        }
        String baseUrl = destConfig != null && !destConfig.isBlank() ? destConfig : config.getDefaultRestBaseUrl();
        return new RestDestination(baseUrl);
    }

    private static TableIdentifier parseTableId(String tablePath) {
        int dot = tablePath.indexOf('.');
        if (dot <= 0 || dot == tablePath.length() - 1) {
            return TableIdentifier.of("default", tablePath);
        }
        return TableIdentifier.of(tablePath.substring(0, dot), tablePath.substring(dot + 1));
    }
}
