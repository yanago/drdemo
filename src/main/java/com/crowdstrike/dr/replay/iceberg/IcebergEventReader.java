package com.crowdstrike.dr.replay.iceberg;

import com.crowdstrike.dr.replay.model.SecurityEvent;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Reads security events from an Iceberg table with optional time range filter.
 */
public final class IcebergEventReader {

    private final Table table;
    private final Long startEventTime;
    private final Long endEventTime;

    public IcebergEventReader(Table table, Long startEventTime, Long endEventTime) {
        this.table = table;
        this.startEventTime = startEventTime;
        this.endEventTime = endEventTime;
    }

    /**
     * Open a closeable iterable of events. Caller must close it.
     */
    public CloseableIterable<SecurityEvent> open() {
        var scan = IcebergGenerics.read(table);
        if (startEventTime != null) {
            scan = scan.where(Expressions.greaterThanOrEqual("event_time", startEventTime));
        }
        if (endEventTime != null) {
            scan = scan.where(Expressions.lessThanOrEqual("event_time", endEventTime));
        }
        CloseableIterable<Record> records = scan.build();
        return new TransformCloseableIterable<>(records, this::recordToEvent);
    }

    /**
     * Read all events into a list. Use only when the dataset fits in memory.
     */
    public List<SecurityEvent> readAll() {
        List<SecurityEvent> out = new ArrayList<>();
        try (CloseableIterable<SecurityEvent> it = open()) {
            for (SecurityEvent e : it) {
                out.add(e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read events", e);
        }
        return out;
    }

    public Iterator<SecurityEvent> iterator() {
        return open().iterator();
    }

    public Stream<SecurityEvent> stream() {
        CloseableIterable<SecurityEvent> iterable = open();
        Iterator<SecurityEvent> it = iterable.iterator();
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED),
                        false)
                .onClose(() -> {
                    try {
                        iterable.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private SecurityEvent recordToEvent(Record r) {
        String cid = getString(r, "cid");
        String eventTimestamp = getString(r, "event_timestamp");
        Long eventTime = (Long) r.getField("event_time");
        String eventType = getString(r, "event_type");
        String eventId = getString(r, "event_id");
        return new SecurityEvent(
                cid != null ? cid : "",
                eventTimestamp != null ? eventTimestamp : "",
                eventTime != null ? eventTime : 0L,
                eventType != null ? eventType : "",
                eventId != null ? eventId : ""
        );
    }

    private static String getString(Record r, String name) {
        Object o = r.getField(name);
        return o == null ? null : o.toString();
    }
}
