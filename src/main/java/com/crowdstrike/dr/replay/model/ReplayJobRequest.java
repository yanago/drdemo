package com.crowdstrike.dr.replay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for creating a replay job.
 */
public final class ReplayJobRequest {

    @JsonProperty("table_path")
    private String tablePath;

    @JsonProperty("destination_type")
    private String destinationType; // "KAFKA" or "REST"

    @JsonProperty("destination_config")
    private String destinationConfig; // topic name or base URL

    @JsonProperty("speed_multiplier")
    private double speedMultiplier = 1.0;

    @JsonProperty("start_event_time")
    private Long startEventTime;

    @JsonProperty("end_event_time")
    private Long endEventTime;

    public String getTablePath() { return tablePath; }
    public void setTablePath(String tablePath) { this.tablePath = tablePath; }
    public String getDestinationType() { return destinationType; }
    public void setDestinationType(String destinationType) { this.destinationType = destinationType; }
    public String getDestinationConfig() { return destinationConfig; }
    public void setDestinationConfig(String destinationConfig) { this.destinationConfig = destinationConfig; }
    public double getSpeedMultiplier() { return speedMultiplier; }
    public void setSpeedMultiplier(double speedMultiplier) { this.speedMultiplier = speedMultiplier; }
    public Long getStartEventTime() { return startEventTime; }
    public void setStartEventTime(Long startEventTime) { this.startEventTime = startEventTime; }
    public Long getEndEventTime() { return endEventTime; }
    public void setEndEventTime(Long endEventTime) { this.endEventTime = endEventTime; }
}
