package com.drdemo.actors;

import com.drdemo.actors.messages.JobCommand;
import com.drdemo.model.ReplayJob;
import com.drdemo.model.SecurityEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Actor responsible for POSTing events to REST endpoints.
 * Uses Java 11+ HttpClient with retry logic.
 */
public class RestWriterActor extends AbstractBehavior<RestWriterActor.WriteBatch> {

    private static final Logger log = LoggerFactory.getLogger(RestWriterActor.class);

    public record WriteBatch(
            String jobId,
            List<SecurityEvent> events,
            ActorRef<JobCommand> replyTo
    ) implements Serializable {}

    private final String endpoint;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private int consecutiveErrors = 0;
    private static final int MAX_CONSECUTIVE_ERRORS = 5;

    public static Behavior<WriteBatch> create(ReplayJob job, Config config) {
        return Behaviors.setup(ctx -> new RestWriterActor(ctx, job, config));
    }

    private RestWriterActor(ActorContext<WriteBatch> context, ReplayJob job, Config config) {
        super(context);
        this.endpoint = job.getRestEndpoint() != null
                ? job.getRestEndpoint()
                : "http://mock-rest-consumer:8081/events";
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        log.info("RestWriterActor initialized for job {} -> endpoint {}", job.getId(), endpoint);
    }

    @Override
    public Receive<WriteBatch> createReceive() {
        return newReceiveBuilder()
                .onMessage(WriteBatch.class, this::onWriteBatch)
                .build();
    }

    private Behavior<WriteBatch> onWriteBatch(WriteBatch cmd) {
        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
            log.error("Too many consecutive REST errors for job {}. Failing batch.", cmd.jobId());
            cmd.replyTo().tell(new JobCommand.BatchFailed(cmd.jobId(),
                    "Too many consecutive REST errors"));
            return this;
        }

        try {
            String body = mapper.writeValueAsString(cmd.events());
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .header("X-Job-Id", cmd.jobId())
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                consecutiveErrors = 0;
                String lastId = cmd.events().isEmpty() ? null
                        : cmd.events().get(cmd.events().size() - 1).getEventId();
                log.debug("POSTed {} events to {}, status={}",
                        cmd.events().size(), endpoint, response.statusCode());
                cmd.replyTo().tell(new JobCommand.BatchCompleted(
                        cmd.jobId(), cmd.events().size(), lastId));
            } else {
                consecutiveErrors++;
                log.warn("REST endpoint returned status {} for job {}",
                        response.statusCode(), cmd.jobId());
                cmd.replyTo().tell(new JobCommand.BatchFailed(cmd.jobId(),
                        "HTTP " + response.statusCode()));
            }
        } catch (Exception e) {
            consecutiveErrors++;
            log.error("REST write failed for job {}: {}", cmd.jobId(), e.getMessage());
            cmd.replyTo().tell(new JobCommand.BatchFailed(cmd.jobId(), e.getMessage()));
        }
        return this;
    }
}
