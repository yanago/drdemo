package com.crowdstrike.dr.replay;

import com.crowdstrike.dr.replay.actor.ReplayGuardian;
import com.crowdstrike.dr.replay.api.ReplayApi;
import com.crowdstrike.dr.replay.config.ReplayConfig;
import com.crowdstrike.dr.replay.iceberg.IcebergCatalogFactory;
import com.crowdstrike.dr.replay.iceberg.TestDataGenerator;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import java.util.concurrent.TimeUnit;

/**
 * Entry point: starts Pekko, ensures test data, and the REST API.
 */
public final class Main {

    public static void main(String[] args) {
        ReplayConfig config = ReplayConfig.fromEnv();
        var catalog = IcebergCatalogFactory.createHadoopCatalog(config.getIcebergWarehousePath());

        // Ensure we have 50k+ events in security.events for demo
        var generator = new TestDataGenerator(catalog, "security.events");
        try {
            generator.ensureTestData(config.getIcebergWarehousePath());
        } catch (Exception e) {
            System.err.println("WARN: Could not ensure test data: " + e.getMessage());
        }

        ActorSystem<Void> actorSystem = ActorSystem.create(
                Behaviors.setup(ctx -> {
                    var guardian = ctx.spawn(ReplayGuardian.create(config), "replay-guardian");
                    ReplayApi api = new ReplayApi(guardian, ctx.getSystem());
                    int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
                    api.app().start(port);
                    ctx.getLog().info("Replay API started on port {}", port);
                    return Behaviors.ignore();
                }),
                "replay-system"
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            actorSystem.terminate();
            try {
                actorSystem.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }));
    }
}
