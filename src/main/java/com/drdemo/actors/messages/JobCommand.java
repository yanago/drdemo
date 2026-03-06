package com.drdemo.actors.messages;

import com.drdemo.model.ReplayJob;
import org.apache.pekko.actor.typed.ActorRef;

import java.io.Serializable;

/**
 * All commands sent to ReplayJobActor via the JobSupervisorActor.
 */
public interface JobCommand extends Serializable {

    // --- Supervisor-level commands ---

    record CreateJob(ReplayJob job, ActorRef<JobResponse> replyTo) implements JobCommand {}
    record StartJob(String jobId, ActorRef<JobResponse> replyTo) implements JobCommand {}
    record PauseJob(String jobId, ActorRef<JobResponse> replyTo) implements JobCommand {}
    record ResumeJob(String jobId, ActorRef<JobResponse> replyTo) implements JobCommand {}
    record CancelJob(String jobId, ActorRef<JobResponse> replyTo) implements JobCommand {}
    record GetJobStatus(String jobId, ActorRef<JobResponse> replyTo) implements JobCommand {}
    record GetJobMetrics(String jobId, ActorRef<JobResponse> replyTo) implements JobCommand {}
    record ListJobs(ActorRef<JobResponse> replyTo) implements JobCommand {}
    record GetJobDetails(String jobId, ActorRef<JobResponse> replyTo) implements JobCommand {}

    // --- Internal actor-to-actor signals ---

    /** Sent by ReplayJobActor to itself to process next batch */
    record ProcessNextBatch(String jobId) implements JobCommand {}

    /** Sent when a batch completes successfully */
    record BatchCompleted(String jobId, int eventsProcessed, String lastEventId) implements JobCommand {}

    /** Sent when a batch fails */
    record BatchFailed(String jobId, String reason) implements JobCommand {}

    /** Sent when job finishes all events */
    record JobCompleted(String jobId) implements JobCommand {}

    /** Periodic metrics snapshot request */
    record CollectMetrics(String jobId) implements JobCommand {}
}
