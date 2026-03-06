package com.crowdstrike.dr.replay.actor;

import com.crowdstrike.dr.replay.model.ReplayJob;

/**
 * Commands for the per-job replay actor.
 */
public sealed interface ReplayJobCommand {

    record Start() implements ReplayJobCommand {}
    record Pause() implements ReplayJobCommand {}
    record Resume() implements ReplayJobCommand {}
    record Cancel() implements ReplayJobCommand {}
    record GetStatus(org.apache.pekko.actor.typed.ActorRef<ReplayJob> replyTo) implements ReplayJobCommand {}
    record GetMetrics(org.apache.pekko.actor.typed.ActorRef<ReplayJob> replyTo) implements ReplayJobCommand {}
    record ReplayProgress(long read, long sent, long failed) implements ReplayJobCommand {}
    record ReplayCompleted() implements ReplayJobCommand {}
    record ReplayFailed(String message) implements ReplayJobCommand {}
}
