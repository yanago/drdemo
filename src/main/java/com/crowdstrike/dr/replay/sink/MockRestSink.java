package com.crowdstrike.dr.replay.sink;

import com.crowdstrike.dr.replay.model.SecurityEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Minimal REST server that accepts POST /events for demo (replay destination).
 * Run as a separate process or in the same JVM for testing.
 */
public final class MockRestSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<SecurityEvent> RECEIVED = new CopyOnWriteArrayList<>();

    public static List<SecurityEvent> getReceived() {
        return new ArrayList<>(RECEIVED);
    }

    public static int getCount() {
        return RECEIVED.size();
    }

    public static Javalin start(int port) {
        return Javalin.create()
                .post("/events", ctx -> {
                    try {
                        SecurityEvent event = MAPPER.readValue(ctx.body(), SecurityEvent.class);
                        RECEIVED.add(event);
                        ctx.status(201);
                    } catch (Exception e) {
                        ctx.status(400).result(e.getMessage());
                    }
                })
                .get("/events/count", ctx -> ctx.json(Map.of("count", RECEIVED.size())))
                .start(port);
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("MOCK_SINK_PORT", "8081"));
        start(port);
        System.out.println("Mock REST sink listening on port " + port + " (POST /events, GET /events/count)");
    }
}
