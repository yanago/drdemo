package com.crowdstrike.dr.replay.iceberg;

import com.crowdstrike.dr.replay.model.SecurityEvent;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.Files;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates at least 50,000 security events into an Iceberg table.
 * Creates the table if it does not exist.
 */
public final class TestDataGenerator {

    private static final String[] EVENT_TYPES = {
            "ProcessStart", "ProcessStop", "NetworkConnect", "NetworkDisconnect",
            "FileCreate", "FileModify", "FileDelete", "RegistrySet", "RegistryDelete",
            "DriverLoad", "ModuleLoad", "DnsQuery", "HttpRequest"
    };

    private static final int BATCH_SIZE = 5_000;
    private static final int MIN_EVENTS = 50_000;

    private final Catalog catalog;
    private final String tablePath; // e.g. "security.events"

    public TestDataGenerator(Catalog catalog, String tablePath) {
        this.catalog = catalog;
        this.tablePath = tablePath;
    }

    /**
     * Ensure table exists and has at least 50,000 events; generate data if not.
     */
    public void ensureTestData(String warehousePath) {
        TableIdentifier id = parseTableId(tablePath);
        Table table;
        if (!catalog.tableExists(id)) {
            table = catalog.createTable(id, IcebergSchema.SECURITY_EVENTS, PartitionSpec.unpartitioned());
        } else {
            table = catalog.loadTable(id);
        }

        long existingRows = countRows(table);
        if (existingRows >= MIN_EVENTS) {
            return;
        }

        int toGenerate = (int) (MIN_EVENTS - existingRows);
        List<Record> batch = new ArrayList<>(BATCH_SIZE);
        GenericRecord template = GenericRecord.create(IcebergSchema.SECURITY_EVENTS);

        for (int i = 0; i < toGenerate; i++) {
            batch.add(toRecord(template, nextEvent()));
            if (batch.size() >= BATCH_SIZE) {
                try {
                    appendBatch(table, batch, warehousePath);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to append batch", e);
                }
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            try {
                appendBatch(table, batch, warehousePath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to append batch", e);
            }
        }
    }

    private SecurityEvent nextEvent() {
        long eventTime = System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(0, 365L * 24 * 60 * 60 * 1000);
        Instant instant = Instant.ofEpochMilli(eventTime);
        String ts = DateTimeFormatter.ISO_INSTANT.format(instant);
        String cid = "customer-" + (char) ('a' + ThreadLocalRandom.current().nextInt(26))
                + ThreadLocalRandom.current().nextInt(1000, 9999)
                + (char) ('a' + ThreadLocalRandom.current().nextInt(26))
                + ThreadLocalRandom.current().nextInt(100, 999);
        String eventType = EVENT_TYPES[ThreadLocalRandom.current().nextInt(EVENT_TYPES.length)];
        String eventId = UUID.randomUUID().toString();
        return new SecurityEvent(cid, ts, eventTime, eventType, eventId);
    }

    private static Record toRecord(GenericRecord template, SecurityEvent e) {
        return template.copy(Map.of(
                "cid", e.getCid(),
                "event_timestamp", e.getEventTimestamp(),
                "event_time", e.getEventTime(),
                "event_type", e.getEventType(),
                "event_id", e.getEventId()
        ));
    }

    private void appendBatch(Table table, List<Record> batch, String warehousePath) throws IOException {
        String tableLocation = table.location();
        if (tableLocation.startsWith("file://")) {
            tableLocation = tableLocation.substring(7);
        }
        File dataDir = new File(tableLocation, "data");
        dataDir.mkdirs();
        File outputFile = new File(dataDir, UUID.randomUUID() + ".parquet");
        OutputFile out = Files.localOutput(outputFile);

        DataWriter<Record> writer = Parquet.writeData(out)
                .schema(IcebergSchema.SECURITY_EVENTS)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .withSpec(PartitionSpec.unpartitioned())
                .build();

        try {
            for (Record r : batch) {
                writer.write(r);
            }
        } finally {
            writer.close();
        }

        AppendFiles append = table.newAppend();
        append.appendFile(writer.toDataFile());
        append.commit();
    }

    private long countRows(Table table) {
        try (var iter = org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
            long count = 0;
            for (Object ignored : iter) {
                count++;
                if (count >= MIN_EVENTS) return count;
            }
            return count;
        } catch (Exception e) {
            return 0;
        }
    }

    private static TableIdentifier parseTableId(String tablePath) {
        int dot = tablePath.indexOf('.');
        if (dot <= 0 || dot == tablePath.length() - 1) {
            return TableIdentifier.of("default", tablePath);
        }
        return TableIdentifier.of(tablePath.substring(0, dot), tablePath.substring(dot + 1));
    }
}
