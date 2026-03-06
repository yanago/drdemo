package com.crowdstrike.dr.replay.config;

/**
 * Configuration for the replay service (catalog, destinations).
 */
public final class ReplayConfig {

    private final String icebergWarehousePath;
    private final String kafkaBootstrapServers;
    private final String defaultRestBaseUrl;

    public ReplayConfig(
            String icebergWarehousePath,
            String kafkaBootstrapServers,
            String defaultRestBaseUrl) {
        this.icebergWarehousePath = icebergWarehousePath;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.defaultRestBaseUrl = defaultRestBaseUrl;
    }

    public String getIcebergWarehousePath() { return icebergWarehousePath; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getDefaultRestBaseUrl() { return defaultRestBaseUrl; }

    public static ReplayConfig fromEnv() {
        String warehouse = System.getenv().getOrDefault("ICEBERG_WAREHOUSE", "file:///data/warehouse");
        String kafka = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String rest = System.getenv().getOrDefault("REST_DESTINATION_BASE_URL", "http://localhost:8081");
        return new ReplayConfig(warehouse, kafka, rest);
    }
}
