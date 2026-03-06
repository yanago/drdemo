package com.drdemo.actors;

import com.drdemo.actors.messages.JobCommand;
import com.drdemo.actors.messages.JobResponse;
import com.drdemo.iceberg.IcebergCatalogManager;
import com.drdemo.model.JobMetrics;
import com.drdemo.model.ReplayJob;
import com.drdemo.model.SecurityEvent;
import com.typesafe.config.Config;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Manages a single replay job's lifecycle.
 * Uses timers for rate-controlled batch processing.
 * Delegates I/O to child actors (IcebergReaderActor, KafkaWriterActor, RestWriterActor).
 */
public class ReplayJobActor extends AbstractBehavior<JobCommand> {

    private static final Logger log = LoggerFactory.getLogger(ReplayJobActor.class);
    private static final String BATCH_TIMER_KEY = "batch-timer";
    private static final String METRICS_TIMER_KEY = "metrics-timer";

    private final ReplayJob job;
    private final Config config;
    private final JobMetrics metrics;

    private ActorRef<JobCommand> icebergReader;
    private ActorRef<JobCommand> kafkaWriter;
    private ActorRef<JobCommand> restWriter;

    // Rate control
    private long batchIntervalMs;
    private Instant jobStartedAt;
    private long batchesProcessed = 0;
    private Iterator<List<SecurityEvent>> batchIterator;
    private boolean exhausted = false;

    public static Behavior<JobCommand> create(ReplayJob job, Config config) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new ReplayJobActor(ctx, timers, job, config)));
    }

    private ReplayJobActor(ActorContext<JobCommand> context,
                           TimerScheduler<JobCommand> timers,
                           ReplayJob job,
                           Config config) {
        super(context);
        this.job = job;
        this.config = config;
        this.metrics = new JobMetrics(job.getId());
        computeBatchInterval();
        log.info("ReplayJobActor created for job {} - {}", job.getId(), job.getName());
    }

    private void computeBatchInterval() {
        // How long to wait between batches to achieve target rate
        int ratePerSec = job.getRatePerSecond() > 0 ? job.getRatePerSecond() : 1000;
        int batchSize = job.getBatchSize() > 0 ? job.getBatchSize() : 500;
        batchIntervalMs = Math.max(1, (long) ((batchSize / (double) ratePerSec) * 1000));
    }

    @Override
    public Receive<JobCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(JobCommand.StartJob.class, this::onStart)
                .onMessage(JobCommand.PauseJob.class, this::onPause)
                .onMessage(JobCommand.ResumeJob.class, this::onResume)
                .onMessage(JobCommand.CancelJob.class, this::onCancel)
                .onMessage(JobCommand.GetJobStatus.class, this::onGetStatus)
                .onMessage(JobCommand.GetJobDetails.class, this::onGetDetails)
                .onMessage(JobCommand.GetJobMetrics.class, this::onGetMetrics)
                .onMessage(JobCommand.ProcessNextBatch.class, this::onProcessNextBatch)
                .onMessage(JobCommand.BatchCompleted.class, this::onBatchCompleted)
                .onMessage(JobCommand.BatchFailed.class, this::onBatchFailed)
                .onMessage(JobCommand.JobCompleted.class, this::onJobCompleted)
                .onMessage(JobCommand.CollectMetrics.class, this::onCollectMetrics)
                .build();
    }

    private Behavior<JobCommand> onStart(JobCommand.StartJob cmd) {
        if (job.getStatus() != ReplayJob.Status.CREATED) {
            cmd.replyTo().tell(new JobResponse.JobError(job.getId(),
                    "Job cannot be started from state: " + job.getStatus()));
            return this;
        }

        log.info("Starting replay job {}", job.getId());
        job.setStatus(ReplayJob.Status.RUNNING);
        job.setStartedAt(Instant.now());
        jobStartedAt = Instant.now();

        // Spawn child writer actors
        spawnWriterActors();

        // Initialize Iceberg reader and get batch iterator asynchronously
        initializeAndStartBatching();

        // Schedule periodic metrics collection
        getContext().getSelf().tell(new JobCommand.CollectMetrics(job.getId()));

        cmd.replyTo().tell(new JobResponse.JobStarted(job.getId()));
        return this;
    }

    private Behavior<JobCommand> onPause(JobCommand.PauseJob cmd) {
        if (job.getStatus() != ReplayJob.Status.RUNNING) {
            cmd.replyTo().tell(new JobResponse.JobError(job.getId(),
                    "Job is not running, current status: " + job.getStatus()));
            return this;
        }
        log.info("Pausing job {}", job.getId());
        job.setStatus(ReplayJob.Status.PAUSED);
        cmd.replyTo().tell(new JobResponse.JobPaused(job.getId()));
        return this;
    }

    private Behavior<JobCommand> onResume(JobCommand.ResumeJob cmd) {
        if (job.getStatus() != ReplayJob.Status.PAUSED) {
            cmd.replyTo().tell(new JobResponse.JobError(job.getId(),
                    "Job is not paused, current status: " + job.getStatus()));
            return this;
        }
        log.info("Resuming job {}", job.getId());
        job.setStatus(ReplayJob.Status.RUNNING);
        // Re-schedule next batch
        getContext().getSelf().tell(new JobCommand.ProcessNextBatch(job.getId()));
        cmd.replyTo().tell(new JobResponse.JobResumed(job.getId()));
        return this;
    }

    private Behavior<JobCommand> onCancel(JobCommand.CancelJob cmd) {
        log.info("Cancelling job {}", job.getId());
        job.setStatus(ReplayJob.Status.CANCELLED);
        job.setCompletedAt(Instant.now());
        cmd.replyTo().tell(new JobResponse.JobCancelled(job.getId()));
        return Behaviors.stopped();
    }

    private Behavior<JobCommand> onGetStatus(JobCommand.GetJobStatus cmd) {
        cmd.replyTo().tell(new JobResponse.JobStatus(job));
        return this;
    }

    private Behavior<JobCommand> onGetDetails(JobCommand.GetJobDetails cmd) {
        cmd.replyTo().tell(new JobResponse.JobDetails(job));
        return this;
    }

    private Behavior<JobCommand> onGetMetrics(JobCommand.GetJobMetrics cmd) {
        updateMetricsSnapshot();
        cmd.replyTo().tell(new JobResponse.MetricsSnapshot(metrics));
        return this;
    }

    private Behavior<JobCommand> onProcessNextBatch(JobCommand.ProcessNextBatch cmd) {
        if (job.getStatus() != ReplayJob.Status.RUNNING) {
            // Paused or cancelled - don't process
            return this;
        }
        if (exhausted || batchIterator == null || !batchIterator.hasNext()) {
            getContext().getSelf().tell(new JobCommand.JobCompleted(job.getId()));
            return this;
        }

        List<SecurityEvent> batch = batchIterator.next();
        if (batch == null || batch.isEmpty()) {
            getContext().getSelf().tell(new JobCommand.JobCompleted(job.getId()));
            return this;
        }

        log.debug("Processing batch {} with {} events for job {}",
                batchesProcessed + 1, batch.size(), job.getId());

        // Fan-out to writer actors
        dispatchBatchToWriters(batch);

        return this;
    }

    private Behavior<JobCommand> onBatchCompleted(JobCommand.BatchCompleted cmd) {
        batchesProcessed++;
        metrics.setEventsRead(metrics.getEventsRead() + cmd.eventsProcessed());
        metrics.setTotalBatches(batchesProcessed);
        metrics.setLastEventId(cmd.lastEventId());
        job.setProcessedEvents(metrics.getEventsRead());
        job.setLastCheckpoint(metrics.getEventsRead());

        log.debug("Batch completed for job {}, total processed: {}",
                job.getId(), metrics.getEventsRead());

        // Rate-controlled scheduling of next batch
        if (job.getStatus() == ReplayJob.Status.RUNNING) {
            // Use a small delay to respect rate limiting
            CompletableFuture.runAsync(() -> {
                try {
                    if (batchIntervalMs > 1) {
                        Thread.sleep(batchIntervalMs);
                    }
                } catch (InterruptedException ignored) {}
                getContext().getSelf().tell(new JobCommand.ProcessNextBatch(job.getId()));
            });
        }
        return this;
    }

    private Behavior<JobCommand> onBatchFailed(JobCommand.BatchFailed cmd) {
        metrics.setEventsFailed(metrics.getEventsFailed() + job.getBatchSize());
        log.warn("Batch failed for job {}: {}", job.getId(), cmd.reason());

        // Continue with next batch despite failure
        if (job.getStatus() == ReplayJob.Status.RUNNING) {
            getContext().getSelf().tell(new JobCommand.ProcessNextBatch(job.getId()));
        }
        return this;
    }

    private Behavior<JobCommand> onJobCompleted(JobCommand.JobCompleted cmd) {
        log.info("Replay job {} completed. Total events processed: {}",
                job.getId(), metrics.getEventsRead());
        job.setStatus(ReplayJob.Status.COMPLETED);
        job.setCompletedAt(Instant.now());
        updateMetricsSnapshot();
        return this;
    }

    private Behavior<JobCommand> onCollectMetrics(JobCommand.CollectMetrics cmd) {
        updateMetricsSnapshot();
        // Reschedule
        if (job.getStatus() == ReplayJob.Status.RUNNING ||
                job.getStatus() == ReplayJob.Status.PAUSED) {
            CompletableFuture.runAsync(() -> {
                try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
                getContext().getSelf().tell(new JobCommand.CollectMetrics(job.getId()));
            });
        }
        return this;
    }

    // ---- Private helpers ----

    private void spawnWriterActors() {
        if (job.getDestinationType() == ReplayJob.DestinationType.KAFKA ||
                job.getDestinationType() == ReplayJob.DestinationType.BOTH) {
            kafkaWriter = getContext().spawn(
                    KafkaWriterActor.create(job, config),
                    "kafka-writer-" + job.getId());
        }
        if (job.getDestinationType() == ReplayJob.DestinationType.REST ||
                job.getDestinationType() == ReplayJob.DestinationType.BOTH) {
            restWriter = getContext().spawn(
                    RestWriterActor.create(job, config),
                    "rest-writer-" + job.getId());
        }
    }

    private void initializeAndStartBatching() {
        // Run Iceberg read initialization in a separate thread to avoid blocking the actor
        ActorRef<JobCommand> self = getContext().getSelf();
        CompletableFuture.runAsync(() -> {
            try {
                IcebergCatalogManager catalogManager = IcebergCatalogManager.getInstance(config);
                batchIterator = catalogManager.getBatchIterator(
                        job.getStartTime(),
                        job.getEndTime(),
                        job.getCustomerIds(),
                        job.getEventTypes(),
                        job.getBatchSize()
                );
                // Count total events (approximate from metadata)
                long total = catalogManager.countEvents(
                        job.getStartTime(), job.getEndTime(),
                        job.getCustomerIds(), job.getEventTypes());
                job.setTotalEvents(total);
                metrics.setEventsRead(0);
                log.info("Iceberg reader initialized for job {}, ~{} events to replay",
                        job.getId(), total);
                // Kick off first batch
                self.tell(new JobCommand.ProcessNextBatch(job.getId()));
            } catch (Exception e) {
                log.error("Failed to initialize Iceberg reader for job {}", job.getId(), e);
                job.setStatus(ReplayJob.Status.FAILED);
                job.setErrorMessage(e.getMessage());
            }
        });
    }

    private void dispatchBatchToWriters(List<SecurityEvent> batch) {
        ActorRef<JobCommand> self = getContext().getSelf();
        String lastEventId = batch.isEmpty() ? null : batch.get(batch.size() - 1).getEventId();

        // Dispatch to Kafka writer
        if (kafkaWriter != null) {
            kafkaWriter.tell(new KafkaWriterActor.WriteBatch(job.getId(), batch, self));
            metrics.setEventsSentKafka(metrics.getEventsSentKafka() + batch.size());
        }

        // Dispatch to REST writer
        if (restWriter != null) {
            restWriter.tell(new RestWriterActor.WriteBatch(job.getId(), batch, self));
            metrics.setEventsSentRest(metrics.getEventsSentRest() + batch.size());
        }

        // If no writers configured, just complete the batch for tracking
        if (kafkaWriter == null && restWriter == null) {
            self.tell(new JobCommand.BatchCompleted(job.getId(), batch.size(), lastEventId));
            return;
        }

        // Acknowledge batch completion after dispatching
        self.tell(new JobCommand.BatchCompleted(job.getId(), batch.size(), lastEventId));
    }

    private void updateMetricsSnapshot() {
        if (jobStartedAt != null) {
            long elapsed = Duration.between(jobStartedAt, Instant.now()).getSeconds();
            metrics.setElapsedSeconds(elapsed);

            if (elapsed > 0) {
                metrics.setAverageRatePerSecond((double) metrics.getEventsRead() / elapsed);
            }

            long total = job.getTotalEvents();
            if (total > 0) {
                double progress = (metrics.getEventsRead() / (double) total) * 100.0;
                metrics.setProgressPercent(Math.min(100.0, progress));

                long remaining = total - metrics.getEventsRead();
                if (metrics.getAverageRatePerSecond() > 0) {
                    metrics.setEstimatedRemainingSeconds(
                            (long) (remaining / metrics.getAverageRatePerSecond()));
                }
            }
        }
        metrics.setSnapshotAt(Instant.now());
    }
}
