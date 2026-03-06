package com.drdemo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Real-time metrics for a replay job.
 */
public class JobMetrics implements Serializable {

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("events_read")
    private long eventsRead;

    @JsonProperty("events_sent_kafka")
    private long eventsSentKafka;

    @JsonProperty("events_sent_rest")
    private long eventsSentRest;

    @JsonProperty("events_failed")
    private long eventsFailed;

    @JsonProperty("events_skipped")
    private long eventsSkipped;

    @JsonProperty("current_rate_per_second")
    private double currentRatePerSecond;

    @JsonProperty("average_rate_per_second")
    private double averageRatePerSecond;

    @JsonProperty("kafka_errors")
    private long kafkaErrors;

    @JsonProperty("rest_errors")
    private long restErrors;

    @JsonProperty("total_batches")
    private long totalBatches;

    @JsonProperty("elapsed_seconds")
    private long elapsedSeconds;

    @JsonProperty("estimated_remaining_seconds")
    private long estimatedRemainingSeconds;

    @JsonProperty("progress_percent")
    private double progressPercent;

    @JsonProperty("last_event_id")
    private String lastEventId;

    @JsonProperty("last_event_timestamp")
    private Instant lastEventTimestamp;

    @JsonProperty("snapshot_at")
    private Instant snapshotAt;

    public JobMetrics() {
        this.snapshotAt = Instant.now();
    }

    public JobMetrics(String jobId) {
        this.jobId = jobId;
        this.snapshotAt = Instant.now();
    }

    // Getters and setters
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public long getEventsRead() { return eventsRead; }
    public void setEventsRead(long eventsRead) { this.eventsRead = eventsRead; }
    public long getEventsSentKafka() { return eventsSentKafka; }
    public void setEventsSentKafka(long eventsSentKafka) { this.eventsSentKafka = eventsSentKafka; }
    public long getEventsSentRest() { return eventsSentRest; }
    public void setEventsSentRest(long eventsSentRest) { this.eventsSentRest = eventsSentRest; }
    public long getEventsFailed() { return eventsFailed; }
    public void setEventsFailed(long eventsFailed) { this.eventsFailed = eventsFailed; }
    public long getEventsSkipped() { return eventsSkipped; }
    public void setEventsSkipped(long eventsSkipped) { this.eventsSkipped = eventsSkipped; }
    public double getCurrentRatePerSecond() { return currentRatePerSecond; }
    public void setCurrentRatePerSecond(double r) { this.currentRatePerSecond = r; }
    public double getAverageRatePerSecond() { return averageRatePerSecond; }
    public void setAverageRatePerSecond(double r) { this.averageRatePerSecond = r; }
    public long getKafkaErrors() { return kafkaErrors; }
    public void setKafkaErrors(long kafkaErrors) { this.kafkaErrors = kafkaErrors; }
    public long getRestErrors() { return restErrors; }
    public void setRestErrors(long restErrors) { this.restErrors = restErrors; }
    public long getTotalBatches() { return totalBatches; }
    public void setTotalBatches(long totalBatches) { this.totalBatches = totalBatches; }
    public long getElapsedSeconds() { return elapsedSeconds; }
    public void setElapsedSeconds(long elapsedSeconds) { this.elapsedSeconds = elapsedSeconds; }
    public long getEstimatedRemainingSeconds() { return estimatedRemainingSeconds; }
    public void setEstimatedRemainingSeconds(long s) { this.estimatedRemainingSeconds = s; }
    public double getProgressPercent() { return progressPercent; }
    public void setProgressPercent(double progressPercent) { this.progressPercent = progressPercent; }
    public String getLastEventId() { return lastEventId; }
    public void setLastEventId(String lastEventId) { this.lastEventId = lastEventId; }
    public Instant getLastEventTimestamp() { return lastEventTimestamp; }
    public void setLastEventTimestamp(Instant lastEventTimestamp) { this.lastEventTimestamp = lastEventTimestamp; }
    public Instant getSnapshotAt() { return snapshotAt; }
    public void setSnapshotAt(Instant snapshotAt) { this.snapshotAt = snapshotAt; }
}
