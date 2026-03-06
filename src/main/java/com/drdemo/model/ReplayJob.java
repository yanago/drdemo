package com.drdemo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Represents a replay job - the core management entity.
 */
public class ReplayJob implements Serializable {

    public enum Status {
        CREATED, RUNNING, PAUSED, COMPLETED, CANCELLED, FAILED
    }

    public enum DestinationType {
        KAFKA, REST, BOTH
    }

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("status")
    private Status status;

    @JsonProperty("destination_type")
    private DestinationType destinationType;

    @JsonProperty("kafka_topic")
    private String kafkaTopic;

    @JsonProperty("rest_endpoint")
    private String restEndpoint;

    // Replay scope
    @JsonProperty("start_time")
    private Instant startTime;

    @JsonProperty("end_time")
    private Instant endTime;

    @JsonProperty("customer_ids")
    private List<String> customerIds;  // null = all customers

    @JsonProperty("event_types")
    private List<String> eventTypes;   // null = all event types

    // Replay control
    @JsonProperty("rate_per_second")
    private int ratePerSecond = 1000;

    @JsonProperty("batch_size")
    private int batchSize = 500;

    // Timestamps
    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("started_at")
    private Instant startedAt;

    @JsonProperty("completed_at")
    private Instant completedAt;

    @JsonProperty("error_message")
    private String errorMessage;

    // Progress tracking
    @JsonProperty("total_events")
    private long totalEvents;

    @JsonProperty("processed_events")
    private long processedEvents;

    @JsonProperty("last_checkpoint")
    private long lastCheckpoint;

    public ReplayJob() {
        this.createdAt = Instant.now();
        this.status = Status.CREATED;
    }

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public DestinationType getDestinationType() { return destinationType; }
    public void setDestinationType(DestinationType destinationType) { this.destinationType = destinationType; }
    public String getKafkaTopic() { return kafkaTopic; }
    public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }
    public String getRestEndpoint() { return restEndpoint; }
    public void setRestEndpoint(String restEndpoint) { this.restEndpoint = restEndpoint; }
    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }
    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }
    public List<String> getCustomerIds() { return customerIds; }
    public void setCustomerIds(List<String> customerIds) { this.customerIds = customerIds; }
    public List<String> getEventTypes() { return eventTypes; }
    public void setEventTypes(List<String> eventTypes) { this.eventTypes = eventTypes; }
    public int getRatePerSecond() { return ratePerSecond; }
    public void setRatePerSecond(int ratePerSecond) { this.ratePerSecond = ratePerSecond; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public long getTotalEvents() { return totalEvents; }
    public void setTotalEvents(long totalEvents) { this.totalEvents = totalEvents; }
    public long getProcessedEvents() { return processedEvents; }
    public void setProcessedEvents(long processedEvents) { this.processedEvents = processedEvents; }
    public long getLastCheckpoint() { return lastCheckpoint; }
    public void setLastCheckpoint(long lastCheckpoint) { this.lastCheckpoint = lastCheckpoint; }
}
