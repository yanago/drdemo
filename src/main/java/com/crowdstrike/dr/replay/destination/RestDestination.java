package com.crowdstrike.dr.replay.destination;

import com.crowdstrike.dr.replay.model.SecurityEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * Sends replayed events to a REST endpoint via HTTP POST.
 */
public class RestDestination implements ReplayDestination {

    private final HttpClient client;
    private final String baseUrl;
    private final ObjectMapper objectMapper;

    public RestDestination(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.objectMapper = new ObjectMapper();
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public CompletionStage<Void> send(SecurityEvent event) {
        try {
            String body = objectMapper.writeValueAsString(event);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/events"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .thenCompose(resp -> {
                        if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                            return java.util.concurrent.CompletableFuture.completedFuture((Void) null);
                        }
                        return java.util.concurrent.CompletableFuture.failedStage(
                                new RuntimeException("HTTP " + resp.statusCode()));
                    });
        } catch (Exception e) {
            return java.util.concurrent.CompletableFuture.failedStage(e);
        }
    }

    @Override
    public void close() {
        // HttpClient doesn't need explicit close for default impl
    }
}
