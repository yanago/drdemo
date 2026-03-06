package com.drdemo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;

import java.io.Serializable;
import java.time.Instant;

/**
 * Core security event domain model.
 * Represents a single event read from the Iceberg data lake.
 */
public class SecurityEvent implements Serializable {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("cid")
    private String cid;

    @JsonProperty("event_timestamp")
    @JsonSerialize(using = InstantSerializer.class)
    @JsonDeserialize(using = InstantDeserializer.INSTANT.class)
    private Instant eventTimestamp;

    @JsonProperty("event_time")
    private long eventTime;

    @JsonProperty("event_type")
    private String eventType;

    // Optional enrichment fields
    @JsonProperty("source_ip")
    private String sourceIp;

    @JsonProperty("destination_ip")
    private String destinationIp;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("process_name")
    private String processName;

    @JsonProperty("host_name")
    private String hostName;

    public SecurityEvent() {}

    public SecurityEvent(String eventId, String cid, Instant eventTimestamp,
                         long eventTime, String eventType) {
        this.eventId = eventId;
        this.cid = cid;
        this.eventTimestamp = eventTimestamp;
        this.eventTime = eventTime;
        this.eventType = eventType;
    }

    // Builder pattern for fluent construction
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final SecurityEvent event = new SecurityEvent();

        public Builder eventId(String eventId) { event.eventId = eventId; return this; }
        public Builder cid(String cid) { event.cid = cid; return this; }
        public Builder eventTimestamp(Instant ts) { event.eventTimestamp = ts; return this; }
        public Builder eventTime(long t) { event.eventTime = t; return this; }
        public Builder eventType(String type) { event.eventType = type; return this; }
        public Builder sourceIp(String ip) { event.sourceIp = ip; return this; }
        public Builder destinationIp(String ip) { event.destinationIp = ip; return this; }
        public Builder severity(String s) { event.severity = s; return this; }
        public Builder processName(String p) { event.processName = p; return this; }
        public Builder hostName(String h) { event.hostName = h; return this; }
        public SecurityEvent build() { return event; }
    }

    // Getters and setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getCid() { return cid; }
    public void setCid(String cid) { this.cid = cid; }
    public Instant getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(Instant eventTimestamp) { this.eventTimestamp = eventTimestamp; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getSourceIp() { return sourceIp; }
    public void setSourceIp(String sourceIp) { this.sourceIp = sourceIp; }
    public String getDestinationIp() { return destinationIp; }
    public void setDestinationIp(String destinationIp) { this.destinationIp = destinationIp; }
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    public String getProcessName() { return processName; }
    public void setProcessName(String processName) { this.processName = processName; }
    public String getHostName() { return hostName; }
    public void setHostName(String hostName) { this.hostName = hostName; }

    @Override
    public String toString() {
        return "SecurityEvent{" +
                "eventId='" + eventId + '\'' +
                ", cid='" + cid + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                '}';
    }
}
