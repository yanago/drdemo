package com.drdemo.actors;

import com.drdemo.iceberg.IcebergCatalogManager;
import com.drdemo.model.SecurityEvent;
import com.typesafe.config.Config;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

/**
 * Stateless actor used for single-shot Iceberg scans.
 * For streaming use, the catalog manager is accessed directly from ReplayJobActor.
 */
public class IcebergReaderActor extends AbstractBehavior<IcebergReaderActor.ReadRequest> {

    private static final Logger log = LoggerFactory.getLogger(IcebergReaderActor.class);

    public record ReadRequest(
            String jobId,
            Instant startTime,
            Instant endTime,
            List<String> customerIds,
            List<String> eventTypes,
            int batchSize
    ) implements Serializable {}

    private final IcebergCatalogManager catalogManager;

    public static Behavior<ReadRequest> create(Config config) {
        return Behaviors.setup(ctx -> new IcebergReaderActor(ctx, config));
    }

    private IcebergReaderActor(ActorContext<ReadRequest> context, Config config) {
        super(context);
        this.catalogManager = IcebergCatalogManager.getInstance(config);
        log.info("IcebergReaderActor initialized");
    }

    @Override
    public Receive<ReadRequest> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReadRequest.class, this::onReadRequest)
                .build();
    }

    private Behavior<ReadRequest> onReadRequest(ReadRequest req) {
        log.info("IcebergReaderActor received read request for job {}", req.jobId());
        try {
            Iterator<List<SecurityEvent>> it = catalogManager.getBatchIterator(
                    req.startTime(), req.endTime(),
                    req.customerIds(), req.eventTypes(),
                    req.batchSize());
            int totalBatches = 0;
            while (it.hasNext()) {
                List<SecurityEvent> batch = it.next();
                totalBatches++;
                log.debug("Read batch {} with {} events", totalBatches, batch.size());
            }
            log.info("IcebergReaderActor completed scan: {} batches for job {}",
                    totalBatches, req.jobId());
        } catch (Exception e) {
            log.error("IcebergReaderActor scan failed for job {}: {}",
                    req.jobId(), e.getMessage(), e);
        }
        return this;
    }
}
