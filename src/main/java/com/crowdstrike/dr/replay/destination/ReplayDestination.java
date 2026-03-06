package com.crowdstrike.dr.replay.destination;

import com.crowdstrike.dr.replay.model.SecurityEvent;

import java.util.concurrent.CompletionStage;

/**
 * Sends replayed security events to a downstream system.
 */
public interface ReplayDestination {

    /**
     * Send a single event. Returns a completion stage that completes when the send is done (or failed).
     */
    CompletionStage<Void> send(SecurityEvent event);

    /**
     * Flush any buffered data and release resources.
     */
    void close();
}
