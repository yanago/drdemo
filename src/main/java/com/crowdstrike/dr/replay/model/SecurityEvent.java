package com.crowdstrike.dr.replay.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Security event schema as required for replay.
 */
public final class SecurityEvent {

    @JsonProperty("cid")
    private final String cid;

    @JsonProperty("event_timestamp")
    private final String eventTimestamp;

    @JsonProperty("event_time")
    private final long eventTime;

    @JsonProperty("event_type")
    private final String eventType;

    @JsonProperty("event_id")
    private final String eventId;

    public SecurityEvent(
            String cid,
            String eventTimestamp,
            long eventTime,
            String eventType,
            String eventId) {
        this.cid = Objects.requireNonNull(cid, "cid");
        this.eventTimestamp = Objects.requireNonNull(eventTimestamp, "event_timestamp");
        this.eventTime = eventTime;
        this.eventType = Objects.requireNonNull(eventType, "event_type");
        this.eventId = Objects.requireNonNull(eventId, "event_id");
    }

    public String getCid() { return cid; }
    public String getEventTimestamp() { return eventTimestamp; }
    public long getEventTime() { return eventTime; }
    public String getEventType() { return eventType; }
    public String getEventId() { return eventId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecurityEvent that = (SecurityEvent) o;
        return eventTime == that.eventTime
                && Objects.equals(cid, that.cid)
                && Objects.equals(eventTimestamp, that.eventTimestamp)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cid, eventTimestamp, eventTime, eventType, eventId);
    }
}
