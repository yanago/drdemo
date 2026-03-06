package com.crowdstrike.dr.replay.actor;

import com.crowdstrike.dr.replay.model.ReplayJob;
import com.crowdstrike.dr.replay.model.ReplayJobRequest;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.List;

/**
 * Commands for the guardian that manages all replay jobs.
 */
public sealed interface ReplayGuardianCommand {

    record CreateJob(ReplayJobRequest request, ActorRef<ReplayJob> replyTo) implements ReplayGuardianCommand {}
    record ListJobs(ActorRef<List<ReplayJob>> replyTo) implements ReplayGuardianCommand {}
    record GetJob(String jobId, ActorRef<ReplayJob> replyTo) implements ReplayGuardianCommand {}
    record StartJob(String jobId, ActorRef<Boolean> replyTo) implements ReplayGuardianCommand {}
    record PauseJob(String jobId, ActorRef<Boolean> replyTo) implements ReplayGuardianCommand {}
    record ResumeJob(String jobId, ActorRef<Boolean> replyTo) implements ReplayGuardianCommand {}
    record CancelJob(String jobId, ActorRef<Boolean> replyTo) implements ReplayGuardianCommand {}
}
