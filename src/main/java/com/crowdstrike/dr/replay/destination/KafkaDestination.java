package com.crowdstrike.dr.replay.destination;

import com.crowdstrike.dr.replay.model.SecurityEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Sends replayed events to a Kafka topic.
 */
public class KafkaDestination implements ReplayDestination {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper objectMapper;

    public KafkaDestination(String bootstrapServers, String topic, Map<String, String> optionalConfig) {
        this.topic = topic;
        this.objectMapper = new ObjectMapper();
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "1");
        props.put("retries", 3);
        if (optionalConfig != null) {
            optionalConfig.forEach(props::put);
        }
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public CompletionStage<Void> send(SecurityEvent event) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        try {
            String value = objectMapper.writeValueAsString(event);
            String key = event.getEventId();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    cf.completeExceptionally(exception);
                } else {
                    cf.complete(null);
                }
            });
        } catch (Exception e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
