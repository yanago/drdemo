package com.crowdstrike.dr.replay.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Replay job definition and state.
 */
public final class ReplayJob {

    public enum Status {
        CREATED,
        RUNNING,
        PAUSED,
        COMPLETED,
        CANCELLED,
        FAILED
    }

    public enum DestinationType {
        KAFKA,
        REST
    }

    private final String id;
    private final String tablePath;
    private final DestinationType destinationType;
    private final String destinationConfig; // topic name or base URL
    private final double speedMultiplier;   // 1.0 = real-time, 2.0 = 2x speed
    private final Long startEventTime;      // optional filter
    private final Long endEventTime;        // optional filter

    private volatile Status status;
    private volatile long eventsRead;
    private volatile long eventsSent;
    private volatile long eventsFailed;
    private volatile String errorMessage;
    private volatile Instant startedAt;
    private volatile Instant completedAt;

    public ReplayJob(
            String id,
            String tablePath,
            DestinationType destinationType,
            String destinationConfig,
            double speedMultiplier,
            Long startEventTime,
            Long endEventTime) {
        this.id = Objects.requireNonNull(id);
        this.tablePath = Objects.requireNonNull(tablePath);
        this.destinationType = Objects.requireNonNull(destinationType);
        this.destinationConfig = Objects.requireNonNull(destinationConfig);
        this.speedMultiplier = speedMultiplier <= 0 ? 1.0 : speedMultiplier;
        this.startEventTime = startEventTime;
        this.endEventTime = endEventTime;
        this.status = Status.CREATED;
        this.eventsRead = 0;
        this.eventsSent = 0;
        this.eventsFailed = 0;
    }

    public String getId() { return id; }
    public String getTablePath() { return tablePath; }
    public DestinationType getDestinationType() { return destinationType; }
    public String getDestinationConfig() { return destinationConfig; }
    public double getSpeedMultiplier() { return speedMultiplier; }
    public Long getStartEventTime() { return startEventTime; }
    public Long getEndEventTime() { return endEventTime; }

    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public long getEventsRead() { return eventsRead; }
    public void setEventsRead(long eventsRead) { this.eventsRead = eventsRead; }
    public long getEventsSent() { return eventsSent; }
    public void setEventsSent(long eventsSent) { this.eventsSent = eventsSent; }
    public long getEventsFailed() { return eventsFailed; }
    public void setEventsFailed(long eventsFailed) { this.eventsFailed = eventsFailed; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }

    public void incrementEventsRead() { this.eventsRead++; }
    public void incrementEventsSent() { this.eventsSent++; }
    public void incrementEventsFailed() { this.eventsFailed++; }
}
