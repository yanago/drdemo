package com.drdemo.kafka;

import com.drdemo.model.SecurityEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Singleton Kafka producer manager.
 *
 * Wraps the Kafka KafkaProducer with:
 *  - Lazy initialization (falls back to mock mode if Kafka is unavailable)
 *  - Batch send with per-record async callbacks
 *  - Graceful shutdown
 *  - Event count tracking
 */
public class KafkaProducerManager {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerManager.class);

    private static volatile KafkaProducerManager instance;

    private KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final String bootstrapServers;
    private final AtomicBoolean mockMode = new AtomicBoolean(false);
    private final AtomicLong totalSent = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    public static KafkaProducerManager getInstance(Config config) {
        if (instance == null) {
            synchronized (KafkaProducerManager.class) {
                if (instance == null) {
                    instance = new KafkaProducerManager(config);
                }
            }
        }
        return instance;
    }

    private KafkaProducerManager(Config config) {
        this.bootstrapServers = config.getString("dr-replay.kafka.bootstrap-servers");
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        try {
            Properties props = buildProducerProperties(config);
            this.producer = new KafkaProducer<>(props);
            // Test connection with a metadata fetch
            this.producer.partitionsFor(config.getString("dr-replay.kafka.topic"));
            log.info("Kafka producer connected to {}", bootstrapServers);
        } catch (Exception e) {
            log.warn("Kafka unavailable at {} - running in MOCK mode. Events will be counted but not sent. Error: {}",
                    bootstrapServers, e.getMessage());
            mockMode.set(true);
            if (producer != null) {
                try { producer.close(); } catch (Exception ignored) {}
                producer = null;
            }
        }
    }

    private Properties buildProducerProperties(Config config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,
                config.hasPath("dr-replay.kafka.acks") ? config.getString("dr-replay.kafka.acks") : "all");
        props.put(ProducerConfig.RETRIES_CONFIG,
                config.hasPath("dr-replay.kafka.retries") ? config.getInt("dr-replay.kafka.retries") : 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                config.hasPath("dr-replay.kafka.batch-size") ? config.getInt("dr-replay.kafka.batch-size") : 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG,
                config.hasPath("dr-replay.kafka.linger-ms") ? config.getInt("dr-replay.kafka.linger-ms") : 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000); // fail fast if broker unavailable
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        return props;
    }

    /**
     * Sends a batch of events to a Kafka topic.
     *
     * @return number of events successfully dispatched (async send)
     */
    public int sendBatch(String topic, List<SecurityEvent> events) throws Exception {
        if (mockMode.get()) {
            // Simulate successful send in mock mode
            totalSent.addAndGet(events.size());
            log.debug("[MOCK-KAFKA] Sent {} events to topic {}", events.size(), topic);
            return events.size();
        }

        int sent = 0;
        for (SecurityEvent event : events) {
            String key = event.getCid(); // partition by customer ID
            String value = mapper.writeValueAsString(event);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    totalErrors.incrementAndGet();
                    log.warn("Failed to send event {} to Kafka: {}",
                            event.getEventId(), exception.getMessage());
                } else {
                    totalSent.incrementAndGet();
                }
            });
            sent++;
        }
        return sent;
    }

    public boolean isMockMode() {
        return mockMode.get();
    }

    public long getTotalSent() {
        return totalSent.get();
    }

    public long getTotalErrors() {
        return totalErrors.get();
    }

    public void close() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close();
                log.info("Kafka producer closed. Total sent: {}, errors: {}",
                        totalSent.get(), totalErrors.get());
            } catch (Exception e) {
                log.warn("Error closing Kafka producer: {}", e.getMessage());
            }
        }
    }
}
