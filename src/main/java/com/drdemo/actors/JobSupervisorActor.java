package com.drdemo.actors;

import com.drdemo.actors.messages.JobCommand;
import com.drdemo.actors.messages.JobResponse;
import com.drdemo.model.ReplayJob;
import com.typesafe.config.Config;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Root supervisor actor. Manages the lifecycle of all ReplayJobActors.
 * One child actor is spawned per replay job.
 */
public class JobSupervisorActor extends AbstractBehavior<JobCommand> {

    private static final Logger log = LoggerFactory.getLogger(JobSupervisorActor.class);

    // Map of jobId -> (job state, child actor ref)
    private final Map<String, ReplayJob> jobs = new HashMap<>();
    private final Map<String, ActorRef<JobCommand>> jobActors = new HashMap<>();

    private final Config config;

    public static Behavior<JobCommand> create(Config config) {
        return Behaviors.setup(ctx -> new JobSupervisorActor(ctx, config));
    }

    private JobSupervisorActor(ActorContext<JobCommand> context, Config config) {
        super(context);
        this.config = config;
        log.info("JobSupervisorActor started");
    }

    @Override
    public Receive<JobCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(JobCommand.CreateJob.class, this::onCreateJob)
                .onMessage(JobCommand.StartJob.class, this::onStartJob)
                .onMessage(JobCommand.PauseJob.class, this::onPauseJob)
                .onMessage(JobCommand.ResumeJob.class, this::onResumeJob)
                .onMessage(JobCommand.CancelJob.class, this::onCancelJob)
                .onMessage(JobCommand.GetJobStatus.class, this::onGetJobStatus)
                .onMessage(JobCommand.GetJobMetrics.class, this::onGetJobMetrics)
                .onMessage(JobCommand.ListJobs.class, this::onListJobs)
                .onMessage(JobCommand.GetJobDetails.class, this::onGetJobDetails)
                .build();
    }

    private Behavior<JobCommand> onCreateJob(JobCommand.CreateJob cmd) {
        String jobId = UUID.randomUUID().toString();
        ReplayJob job = cmd.job();
        job.setId(jobId);
        job.setStatus(ReplayJob.Status.CREATED);

        // Spawn child actor with supervision - restart on failure up to 3 times
        ActorRef<JobCommand> jobActor = getContext().spawn(
                Behaviors.supervise(ReplayJobActor.create(job, config))
                        .onFailure(Exception.class,
                                SupervisorStrategy.restart().withLimit(3, Duration.ofSeconds(30))),
                "job-" + jobId
        );

        jobs.put(jobId, job);
        jobActors.put(jobId, jobActor);

        log.info("Created replay job {} - {}", jobId, job.getName());
        cmd.replyTo().tell(new JobResponse.JobCreated(job));
        return this;
    }

    private Behavior<JobCommand> onStartJob(JobCommand.StartJob cmd) {
        ActorRef<JobCommand> actor = jobActors.get(cmd.jobId());
        if (actor == null) {
            cmd.replyTo().tell(new JobResponse.JobNotFound(cmd.jobId()));
            return this;
        }
        actor.tell(new JobCommand.StartJob(cmd.jobId(), cmd.replyTo()));
        return this;
    }

    private Behavior<JobCommand> onPauseJob(JobCommand.PauseJob cmd) {
        ActorRef<JobCommand> actor = jobActors.get(cmd.jobId());
        if (actor == null) {
            cmd.replyTo().tell(new JobResponse.JobNotFound(cmd.jobId()));
            return this;
        }
        actor.tell(new JobCommand.PauseJob(cmd.jobId(), cmd.replyTo()));
        return this;
    }

    private Behavior<JobCommand> onResumeJob(JobCommand.ResumeJob cmd) {
        ActorRef<JobCommand> actor = jobActors.get(cmd.jobId());
        if (actor == null) {
            cmd.replyTo().tell(new JobResponse.JobNotFound(cmd.jobId()));
            return this;
        }
        actor.tell(new JobCommand.ResumeJob(cmd.jobId(), cmd.replyTo()));
        return this;
    }

    private Behavior<JobCommand> onCancelJob(JobCommand.CancelJob cmd) {
        ActorRef<JobCommand> actor = jobActors.get(cmd.jobId());
        if (actor == null) {
            cmd.replyTo().tell(new JobResponse.JobNotFound(cmd.jobId()));
            return this;
        }
        actor.tell(new JobCommand.CancelJob(cmd.jobId(), cmd.replyTo()));
        // Update local state
        ReplayJob job = jobs.get(cmd.jobId());
        if (job != null) job.setStatus(ReplayJob.Status.CANCELLED);
        return this;
    }

    private Behavior<JobCommand> onGetJobStatus(JobCommand.GetJobStatus cmd) {
        ActorRef<JobCommand> actor = jobActors.get(cmd.jobId());
        if (actor == null) {
            cmd.replyTo().tell(new JobResponse.JobNotFound(cmd.jobId()));
            return this;
        }
        actor.tell(new JobCommand.GetJobStatus(cmd.jobId(), cmd.replyTo()));
        return this;
    }

    private Behavior<JobCommand> onGetJobMetrics(JobCommand.GetJobMetrics cmd) {
        ActorRef<JobCommand> actor = jobActors.get(cmd.jobId());
        if (actor == null) {
            cmd.replyTo().tell(new JobResponse.JobNotFound(cmd.jobId()));
            return this;
        }
        actor.tell(new JobCommand.GetJobMetrics(cmd.jobId(), cmd.replyTo()));
        return this;
    }

    private Behavior<JobCommand> onListJobs(JobCommand.ListJobs cmd) {
        cmd.replyTo().tell(new JobResponse.JobList(new ArrayList<>(jobs.values())));
        return this;
    }

    private Behavior<JobCommand> onGetJobDetails(JobCommand.GetJobDetails cmd) {
        ActorRef<JobCommand> actor = jobActors.get(cmd.jobId());
        if (actor == null) {
            cmd.replyTo().tell(new JobResponse.JobNotFound(cmd.jobId()));
            return this;
        }
        actor.tell(new JobCommand.GetJobDetails(cmd.jobId(), cmd.replyTo()));
        return this;
    }
}
