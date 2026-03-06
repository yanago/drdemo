package com.crowdstrike.dr.replay.actor;

import com.crowdstrike.dr.replay.destination.ReplayDestination;
import com.crowdstrike.dr.replay.iceberg.IcebergEventReader;
import com.crowdstrike.dr.replay.model.ReplayJob;
import com.crowdstrike.dr.replay.model.SecurityEvent;
import org.apache.iceberg.Table;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Actor that runs a single replay job: reads from Iceberg and sends to destination.
 * Handles start, pause, resume, cancel and reports progress.
 */
public final class ReplayJobActor extends AbstractBehavior<ReplayJobCommand> {

    private final ReplayJob job;
    private final Table table;
    private final ReplayDestination destination;
    private final ActorContext<ReplayJobCommand> ctx;

    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile boolean running = false;

    private ReplayJobActor(
            ActorContext<ReplayJobCommand> context,
            ReplayJob job,
            Table table,
            ReplayDestination destination) {
        super(context);
        this.ctx = context;
        this.job = job;
        this.table = table;
        this.destination = destination;
    }

    public static Behavior<ReplayJobCommand> create(
            ReplayJob job,
            Table table,
            ReplayDestination destination) {
        return Behaviors.setup(ctx -> new ReplayJobActor(ctx, job, table, destination));
    }

    @Override
    public Receive<ReplayJobCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReplayJobCommand.Start.class, this::onStart)
                .onMessage(ReplayJobCommand.Pause.class, this::onPause)
                .onMessage(ReplayJobCommand.Resume.class, this::onResume)
                .onMessage(ReplayJobCommand.Cancel.class, this::onCancel)
                .onMessage(ReplayJobCommand.GetStatus.class, this::onGetStatus)
                .onMessage(ReplayJobCommand.GetMetrics.class, this::onGetMetrics)
                .onMessage(ReplayJobCommand.ReplayProgress.class, this::onProgress)
                .onMessage(ReplayJobCommand.ReplayCompleted.class, this::onCompleted)
                .onMessage(ReplayJobCommand.ReplayFailed.class, this::onFailed)
                .build();
    }

    private Behavior<ReplayJobCommand> onStart(ReplayJobCommand.Start msg) {
        if (job.getStatus() != ReplayJob.Status.CREATED) {
            return this;
        }
        job.setStatus(ReplayJob.Status.RUNNING);
        job.setStartedAt(Instant.now());
        running = true;
        ctx.getExecutionContext().execute(this::runReplay);
        return this;
    }

    private Behavior<ReplayJobCommand> onPause(ReplayJobCommand.Pause msg) {
        paused.set(true);
        if (running) {
            job.setStatus(ReplayJob.Status.PAUSED);
        }
        return this;
    }

    private Behavior<ReplayJobCommand> onResume(ReplayJobCommand.Resume msg) {
        paused.set(false);
        if (running) {
            job.setStatus(ReplayJob.Status.RUNNING);
        }
        return this;
    }

    private Behavior<ReplayJobCommand> onCancel(ReplayJobCommand.Cancel msg) {
        cancelled.set(true);
        job.setStatus(ReplayJob.Status.CANCELLED);
        job.setCompletedAt(Instant.now());
        return this;
    }

    private Behavior<ReplayJobCommand> onGetStatus(ReplayJobCommand.GetStatus msg) {
        msg.replyTo().tell(job);
        return this;
    }

    private Behavior<ReplayJobCommand> onGetMetrics(ReplayJobCommand.GetMetrics msg) {
        msg.replyTo().tell(job);
        return this;
    }

    private Behavior<ReplayJobCommand> onProgress(ReplayJobCommand.ReplayProgress msg) {
        job.setEventsRead(msg.read());
        job.setEventsSent(msg.sent());
        job.setEventsFailed(msg.failed());
        return this;
    }

    private Behavior<ReplayJobCommand> onCompleted(ReplayJobCommand.ReplayCompleted msg) {
        running = false;
        job.setStatus(ReplayJob.Status.COMPLETED);
        job.setCompletedAt(Instant.now());
        try {
            destination.close();
        } catch (Exception ignored) {}
        return this;
    }

    private Behavior<ReplayJobCommand> onFailed(ReplayJobCommand.ReplayFailed msg) {
        running = false;
        job.setStatus(ReplayJob.Status.FAILED);
        job.setErrorMessage(msg.message());
        job.setCompletedAt(Instant.now());
        try {
            destination.close();
        } catch (Exception ignored) {}
        return this;
    }

    private void runReplay() {
        try {
            IcebergEventReader reader = new IcebergEventReader(
                    table,
                    job.getStartEventTime(),
                    job.getEndEventTime()
            );
            long read = 0, sent = 0, failed = 0;
            long lastEventTime = 0;
            double speed = job.getSpeedMultiplier();

            try (var iter = reader.open()) {
                for (SecurityEvent event : iter) {
                    if (cancelled.get()) break;
                    while (paused.get() && !cancelled.get()) {
                        Thread.sleep(200);
                    }
                    if (cancelled.get()) break;

                    read++;
                    // Throttle by event time and speed multiplier
                    if (speed > 0 && speed != Double.POSITIVE_INFINITY && lastEventTime > 0) {
                        long delay = (long) ((event.getEventTime() - lastEventTime) / speed);
                        if (delay > 0 && delay < 60_000) {
                            Thread.sleep(delay);
                        }
                    }
                    lastEventTime = event.getEventTime();

                    try {
                        CompletionStage<Void> cs = destination.send(event);
                        cs.toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
                        sent++;
                    } catch (Exception e) {
                        failed++;
                    }

                    if (read % 100 == 0) {
                        ctx.getSelf().tell(new ReplayJobCommand.ReplayProgress(read, sent, failed));
                    }
                }
            }

            if (cancelled.get()) {
                return;
            }
            ctx.getSelf().tell(new ReplayJobCommand.ReplayProgress(read, sent, failed));
            ctx.getSelf().tell(new ReplayJobCommand.ReplayCompleted());
        } catch (Exception e) {
            ctx.getSelf().tell(new ReplayJobCommand.ReplayFailed(e.getMessage()));
        }
    }
}
