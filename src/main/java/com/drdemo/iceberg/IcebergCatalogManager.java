package com.drdemo.iceberg;

import com.drdemo.model.SecurityEvent;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the Apache Iceberg catalog for the DR replay data lake.
 *
 * Uses a local HadoopCatalog backed by the filesystem (configurable to S3/MinIO).
 * Provides:
 *  - Idempotent table creation and data seeding (50k+ events)
 *  - Batch iterator with time/customer/event-type filtering
 *  - Event count estimation for progress tracking
 */
public class IcebergCatalogManager {

    private static final Logger log = LoggerFactory.getLogger(IcebergCatalogManager.class);

    private static final String CATALOG_NAME = "dr_catalog";
    private static final String TABLE_NAMESPACE = "security";
    private static final String TABLE_NAME = "events";
    private static final int SEED_EVENT_COUNT = 55_000;

    // Iceberg schema matching SecurityEvent required fields
    public static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "cid", Types.StringType.get()),
            Types.NestedField.required(3, "event_timestamp", Types.TimestampType.withZone()),
            Types.NestedField.required(4, "event_time", Types.LongType.get()),
            Types.NestedField.required(5, "event_type", Types.StringType.get()),
            Types.NestedField.optional(6, "source_ip", Types.StringType.get()),
            Types.NestedField.optional(7, "destination_ip", Types.StringType.get()),
            Types.NestedField.optional(8, "severity", Types.StringType.get()),
            Types.NestedField.optional(9, "process_name", Types.StringType.get()),
            Types.NestedField.optional(10, "host_name", Types.StringType.get())
    );

    // Partition by day (event_timestamp truncated to day)
    private static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(SCHEMA)
            .day("event_timestamp")
            .build();

    private static volatile IcebergCatalogManager instance;

    private final Catalog catalog;
    private final TableIdentifier tableId;
    private final String warehousePath;

    // In-memory cache for fast re-reads during demo
    // Maps: partition-date -> list of events
    private final Map<String, List<SecurityEvent>> inMemoryStore = new LinkedHashMap<>();
    private boolean seeded = false;

    public static IcebergCatalogManager getInstance(Config config) {
        if (instance == null) {
            synchronized (IcebergCatalogManager.class) {
                if (instance == null) {
                    instance = new IcebergCatalogManager(config);
                }
            }
        }
        return instance;
    }

    private IcebergCatalogManager(Config config) {
        this.warehousePath = config.getString("dr-replay.iceberg.warehouse");
        log.info("Initializing Iceberg catalog at: {}", warehousePath);

        Configuration hadoopConf = new Configuration();

        // Optional S3/MinIO configuration
        if (config.hasPath("dr-replay.iceberg.s3-endpoint")) {
            String s3Endpoint = config.getString("dr-replay.iceberg.s3-endpoint");
            hadoopConf.set("fs.s3a.endpoint", s3Endpoint);
            hadoopConf.set("fs.s3a.path.style.access", "true");
            hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            if (config.hasPath("dr-replay.iceberg.s3-access-key")) {
                hadoopConf.set("fs.s3a.access.key", config.getString("dr-replay.iceberg.s3-access-key"));
                hadoopConf.set("fs.s3a.secret.key", config.getString("dr-replay.iceberg.s3-secret-key"));
            }
            log.info("S3/MinIO backend configured at {}", s3Endpoint);
        }

        this.catalog = new HadoopCatalog(hadoopConf, warehousePath);
        this.tableId = TableIdentifier.of(TABLE_NAMESPACE, TABLE_NAME);

        ensureTableExists();
        log.info("Iceberg catalog initialized. Table: {}.{}", TABLE_NAMESPACE, TABLE_NAME);
    }

    private void ensureTableExists() {
        try {
            if (!catalog.tableExists(tableId)) {
                log.info("Creating Iceberg table {}", tableId);
                catalog.createTable(tableId, SCHEMA, PARTITION_SPEC);
                log.info("Iceberg table created successfully");
            } else {
                log.info("Iceberg table {} already exists", tableId);
            }
        } catch (Exception e) {
            log.warn("Could not create Iceberg table (may already exist or catalog is read-only): {}", e.getMessage());
        }
    }

    /**
     * Seeds the in-memory store with 55,000 security events if not already done.
     * This is the primary data source for the demo — uses in-memory store for speed
     * and reliability without requiring a full Hadoop/Parquet write setup.
     */
    public synchronized void seedIfEmpty() {
        if (seeded) {
            log.info("Iceberg data already seeded ({} events in store)", getTotalEventCount());
            return;
        }

        log.info("Seeding Iceberg in-memory store with {} security events...", SEED_EVENT_COUNT);
        long seedStart = System.currentTimeMillis();

        String[] eventTypes = {
                "ProcessStart", "NetworkConnect", "FileAccess", "LoginAttempt",
                "PrivilegeEscalation", "DataExfiltration", "PortScan",
                "DnsQuery", "CommandExecution", "ServiceInstall"
        };
        String[] severities = {"LOW", "MEDIUM", "HIGH", "CRITICAL"};
        String[] processes = {"svchost.exe", "cmd.exe", "powershell.exe", "python3", "bash", "curl", "wget"};
        String[] hosts = {"ws-001", "ws-002", "srv-db-01", "srv-web-01", "srv-api-01", "dc-01"};

        // Generate customers: 100 distinct customer IDs
        String[] customers = new String[100];
        for (int i = 0; i < 100; i++) {
            customers[i] = "customer-" + UUID.randomUUID().toString().substring(0, 8);
        }

        // Time range: last 30 days
        Instant endTime = Instant.now();
        Instant startTime = endTime.minusSeconds(30L * 24 * 3600);
        long startEpoch = startTime.toEpochMilli();
        long endEpoch = endTime.toEpochMilli();
        long range = endEpoch - startEpoch;

        Random rng = new Random(42); // deterministic seed for reproducibility

        for (int i = 0; i < SEED_EVENT_COUNT; i++) {
            long eventTimeMs = startEpoch + (long) (rng.nextDouble() * range);
            Instant eventTs = Instant.ofEpochMilli(eventTimeMs);
            String dayKey = DateTimeFormatter.ISO_LOCAL_DATE
                    .withZone(ZoneOffset.UTC)
                    .format(eventTs);

            SecurityEvent event = SecurityEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .cid(customers[rng.nextInt(customers.length)])
                    .eventTimestamp(eventTs)
                    .eventTime(eventTimeMs)
                    .eventType(eventTypes[rng.nextInt(eventTypes.length)])
                    .sourceIp(randomIp(rng))
                    .destinationIp(randomIp(rng))
                    .severity(severities[rng.nextInt(severities.length)])
                    .processName(processes[rng.nextInt(processes.length)])
                    .hostName(hosts[rng.nextInt(hosts.length)])
                    .build();

            inMemoryStore.computeIfAbsent(dayKey, k -> new ArrayList<>()).add(event);
        }

        seeded = true;
        long elapsed = System.currentTimeMillis() - seedStart;
        log.info("Seeded {} events across {} day-partitions in {}ms",
                SEED_EVENT_COUNT, inMemoryStore.size(), elapsed);

        // Log partition breakdown
        inMemoryStore.forEach((day, events) ->
                log.debug("  Partition {}: {} events", day, events.size()));
    }

    /**
     * Returns a lazy batch iterator over events matching the given filters.
     * All filtering is done in-memory for the demo.
     *
     * @param startTime   inclusive lower bound (null = no lower bound)
     * @param endTime     inclusive upper bound (null = no upper bound)
     * @param customerIds filter to specific customers (null/empty = all)
     * @param eventTypes  filter to specific event types (null/empty = all)
     * @param batchSize   number of events per batch
     */
    public Iterator<List<SecurityEvent>> getBatchIterator(
            Instant startTime,
            Instant endTime,
            List<String> customerIds,
            List<String> eventTypes,
            int batchSize) {

        // Collect all matching events from in-memory store
        List<SecurityEvent> matching = new ArrayList<>();

        Set<String> cidFilter = (customerIds != null && !customerIds.isEmpty())
                ? new HashSet<>(customerIds) : null;
        Set<String> typeFilter = (eventTypes != null && !eventTypes.isEmpty())
                ? new HashSet<>(eventTypes) : null;

        for (List<SecurityEvent> partition : inMemoryStore.values()) {
            for (SecurityEvent event : partition) {
                if (startTime != null && event.getEventTimestamp().isBefore(startTime)) continue;
                if (endTime != null && event.getEventTimestamp().isAfter(endTime)) continue;
                if (cidFilter != null && !cidFilter.contains(event.getCid())) continue;
                if (typeFilter != null && !typeFilter.contains(event.getEventType())) continue;
                matching.add(event);
            }
        }

        // Sort by event_time for deterministic replay order
        matching.sort(Comparator.comparingLong(SecurityEvent::getEventTime));

        log.info("Batch iterator: {} matching events (startTime={}, endTime={}, customers={}, types={})",
                matching.size(), startTime, endTime,
                cidFilter != null ? cidFilter.size() + " customers" : "all",
                typeFilter != null ? typeFilter : "all");

        int effectiveBatchSize = batchSize > 0 ? batchSize : 500;
        final List<SecurityEvent> events = matching;

        return new Iterator<>() {
            private int offset = 0;

            @Override
            public boolean hasNext() {
                return offset < events.size();
            }

            @Override
            public List<SecurityEvent> next() {
                int end = Math.min(offset + effectiveBatchSize, events.size());
                List<SecurityEvent> batch = events.subList(offset, end);
                offset = end;
                return new ArrayList<>(batch);
            }
        };
    }

    /**
     * Counts events matching the given filters (for progress tracking).
     */
    public long countEvents(
            Instant startTime, Instant endTime,
            List<String> customerIds, List<String> eventTypes) {

        Set<String> cidFilter = (customerIds != null && !customerIds.isEmpty())
                ? new HashSet<>(customerIds) : null;
        Set<String> typeFilter = (eventTypes != null && !eventTypes.isEmpty())
                ? new HashSet<>(eventTypes) : null;

        long count = 0;
        for (List<SecurityEvent> partition : inMemoryStore.values()) {
            for (SecurityEvent event : partition) {
                if (startTime != null && event.getEventTimestamp().isBefore(startTime)) continue;
                if (endTime != null && event.getEventTimestamp().isAfter(endTime)) continue;
                if (cidFilter != null && !cidFilter.contains(event.getCid())) continue;
                if (typeFilter != null && !typeFilter.contains(event.getEventType())) continue;
                count++;
            }
        }
        return count;
    }

    public long getTotalEventCount() {
        return inMemoryStore.values().stream().mapToLong(List::size).sum();
    }

    public boolean isSeeded() {
        return seeded;
    }

    private String randomIp(Random rng) {
        return rng.nextInt(256) + "." + rng.nextInt(256) + "." +
                rng.nextInt(256) + "." + rng.nextInt(256);
    }
}
