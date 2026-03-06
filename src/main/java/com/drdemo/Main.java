package com.drdemo;

import com.drdemo.actors.JobSupervisorActor;
import com.drdemo.actors.messages.JobCommand;
import com.drdemo.http.ApiServer;
import com.drdemo.iceberg.IcebergCatalogManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.typed.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application entry point.
 * Bootstraps:
 *  1. Pekko ActorSystem with JobSupervisorActor as the guardian
 *  2. IcebergCatalogManager (seeds data on first start)
 *  3. Pekko HTTP ApiServer
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();

        log.info("======================================");
        log.info(" DR Replay Service starting up...");
        log.info("======================================");

        // 1. Seed Iceberg data lake on startup (idempotent)
        log.info("Initializing Iceberg catalog and seeding test data...");
        IcebergCatalogManager catalog = IcebergCatalogManager.getInstance(config);
        catalog.seedIfEmpty();
        log.info("Iceberg catalog ready.");

        // 2. Start Pekko actor system — guardian is JobSupervisorActor
        ActorSystem<JobCommand> system = ActorSystem.create(
                JobSupervisorActor.create(config),
                "dr-replay-system",
                config
        );

        log.info("Pekko ActorSystem started.");

        // 3. Start HTTP API server
        ApiServer apiServer = new ApiServer(system, config);
        apiServer.start();

        String host = config.getString("dr-replay.http.host");
        int port = config.getInt("dr-replay.http.port");
        log.info("API server listening on http://{}:{}", host, port);
        log.info("Health check: http://{}:{}/health", host, port);
        log.info("======================================");

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down DR Replay Service...");
            system.terminate();
        }));

        // Block until system terminates
        system.getWhenTerminated().toCompletableFuture().join();
    }
}
