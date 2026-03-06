package com.drdemo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * Request body for POST /api/v1/replay/jobs
 */
public class CreateJobRequest {

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("destination_type")
    private ReplayJob.DestinationType destinationType = ReplayJob.DestinationType.BOTH;

    @JsonProperty("kafka_topic")
    private String kafkaTopic;

    @JsonProperty("rest_endpoint")
    private String restEndpoint;

    @JsonProperty("start_time")
    private Instant startTime;

    @JsonProperty("end_time")
    private Instant endTime;

    @JsonProperty("customer_ids")
    private List<String> customerIds;

    @JsonProperty("event_types")
    private List<String> eventTypes;

    @JsonProperty("rate_per_second")
    private int ratePerSecond = 1000;

    @JsonProperty("batch_size")
    private int batchSize = 500;

    // Getters and setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public ReplayJob.DestinationType getDestinationType() { return destinationType; }
    public void setDestinationType(ReplayJob.DestinationType d) { this.destinationType = d; }
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
}
