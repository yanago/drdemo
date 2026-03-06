package com.drdemo.actors;

import com.drdemo.actors.messages.JobCommand;
import com.drdemo.kafka.KafkaProducerManager;
import com.drdemo.model.SecurityEvent;
import com.typesafe.config.Config;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Actor responsible for writing batches of events to Kafka.
 * Runs on the dedicated kafka-dispatcher.
 */
public class KafkaWriterActor extends AbstractBehavior<KafkaWriterActor.WriteBatch> {

    private static final Logger log = LoggerFactory.getLogger(KafkaWriterActor.class);

    // Command type specific to this actor
    public record WriteBatch(
            String jobId,
            List<SecurityEvent> events,
            ActorRef<JobCommand> replyTo
    ) implements Serializable {}

    private final KafkaProducerManager producerManager;
    private final String topic;

    public static Behavior<WriteBatch> create(com.drdemo.model.ReplayJob job, Config config) {
        return Behaviors.setup(ctx -> new KafkaWriterActor(ctx, job, config));
    }

    private KafkaWriterActor(ActorContext<WriteBatch> context,
                             com.drdemo.model.ReplayJob job,
                             Config config) {
        super(context);
        this.producerManager = KafkaProducerManager.getInstance(config);
        this.topic = job.getKafkaTopic() != null
                ? job.getKafkaTopic()
                : config.getString("dr-replay.kafka.topic");
        log.info("KafkaWriterActor initialized for job {} -> topic {}", job.getId(), topic);
    }

    @Override
    public Receive<WriteBatch> createReceive() {
        return newReceiveBuilder()
                .onMessage(WriteBatch.class, this::onWriteBatch)
                .build();
    }

    private Behavior<WriteBatch> onWriteBatch(WriteBatch cmd) {
        try {
            int sent = producerManager.sendBatch(topic, cmd.events());
            log.debug("Sent {} events to Kafka topic {}", sent, topic);
            String lastId = cmd.events().isEmpty() ? null
                    : cmd.events().get(cmd.events().size() - 1).getEventId();
            cmd.replyTo().tell(new JobCommand.BatchCompleted(cmd.jobId(), sent, lastId));
        } catch (Exception e) {
            log.error("Failed to send batch to Kafka for job {}: {}", cmd.jobId(), e.getMessage(), e);
            cmd.replyTo().tell(new JobCommand.BatchFailed(cmd.jobId(), e.getMessage()));
        }
        return this;
    }
}
