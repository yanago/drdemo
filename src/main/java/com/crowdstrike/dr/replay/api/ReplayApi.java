package com.crowdstrike.dr.replay.api;

import com.crowdstrike.dr.replay.actor.ReplayGuardianCommand;
import com.crowdstrike.dr.replay.model.ReplayJob;
import com.crowdstrike.dr.replay.model.ReplayJobRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * REST API for replay job management.
 */
public final class ReplayApi {

    private final ActorRef<ReplayGuardianCommand> guardian;
    private final ActorSystem<?> system;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final java.time.Duration askTimeout = Duration.ofSeconds(10);

    public ReplayApi(ActorRef<ReplayGuardianCommand> guardian, ActorSystem<?> system) {
        this.guardian = guardian;
        this.system = system;
    }

    public Javalin app() {
        Javalin javalin = Javalin.create(javalinConfig -> {
            javalinConfig.http.asyncTimeout = 15_000L;
        });

        // Health and metrics
        javalin.get("/health", this::health);
        javalin.get("/metrics", this::metrics);

        // Replay job API
        javalin.post("/api/v1/replay/jobs", this::createJob);
        javalin.get("/api/v1/replay/jobs", this::listJobs);
        javalin.get("/api/v1/replay/jobs/{id}", this::getJob);
        javalin.post("/api/v1/replay/jobs/{id}/start", this::startJob);
        javalin.post("/api/v1/replay/jobs/{id}/pause", this::pauseJob);
        javalin.post("/api/v1/replay/jobs/{id}/resume", this::resumeJob);
        javalin.post("/api/v1/replay/jobs/{id}/cancel", this::cancelJob);
        javalin.get("/api/v1/replay/jobs/{id}/status", this::getStatus);
        javalin.get("/api/v1/replay/jobs/{id}/metrics", this::getMetrics);

        return javalin;
    }

    private void health(Context ctx) {
        ctx.json(Map.of("status", "UP"));
    }

    private void metrics(Context ctx) {
        ctx.json(Map.of(
                "service", "disaster-recovery-replay",
                "version", "1.0"
        ));
    }

    private void createJob(Context ctx) {
        try {
            ReplayJobRequest req = objectMapper.readValue(ctx.body(), ReplayJobRequest.class);
            ReplayJob job = AskPattern.<ReplayGuardianCommand, ReplayJob>ask(guardian, (ActorRef<ReplayJob> replyTo) -> new ReplayGuardianCommand.CreateJob(req, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            ctx.json(job).status(201);
        } catch (Exception e) {
            if (e.getCause() != null) {
                ctx.status(500).json(Map.of("error", e.getCause().getMessage()));
            } else {
                ctx.status(400).json(Map.of("error", e.getMessage()));
            }
        }
    }

    private void listJobs(Context ctx) {
        try {
            List<ReplayJob> list = AskPattern.<ReplayGuardianCommand, List<ReplayJob>>ask(guardian, (ActorRef<List<ReplayJob>> replyTo) -> new ReplayGuardianCommand.ListJobs(replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            ctx.json(list);
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private void getJob(Context ctx) {
        String id = ctx.pathParam("id");
        try {
            ReplayJob job = AskPattern.<ReplayGuardianCommand, ReplayJob>ask(guardian, (ActorRef<ReplayJob> replyTo) -> new ReplayGuardianCommand.GetJob(id, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            if (job != null) {
                ctx.json(job);
            } else {
                ctx.status(404).json(Map.of("error", "Job not found"));
            }
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private void startJob(Context ctx) {
        String id = ctx.pathParam("id");
        try {
            Boolean ok = AskPattern.<ReplayGuardianCommand, Boolean>ask(guardian, (ActorRef<Boolean> replyTo) -> new ReplayGuardianCommand.StartJob(id, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            if (Boolean.TRUE.equals(ok)) {
                ctx.status(202).json(Map.of("message", "Start requested"));
            } else {
                ctx.status(404).json(Map.of("error", "Job not found"));
            }
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private void pauseJob(Context ctx) {
        String id = ctx.pathParam("id");
        try {
            Boolean ok = AskPattern.<ReplayGuardianCommand, Boolean>ask(guardian, (ActorRef<Boolean> replyTo) -> new ReplayGuardianCommand.PauseJob(id, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            if (Boolean.TRUE.equals(ok)) {
                ctx.status(202).json(Map.of("message", "Pause requested"));
            } else {
                ctx.status(404).json(Map.of("error", "Job not found"));
            }
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private void resumeJob(Context ctx) {
        String id = ctx.pathParam("id");
        try {
            Boolean ok = AskPattern.<ReplayGuardianCommand, Boolean>ask(guardian, (ActorRef<Boolean> replyTo) -> new ReplayGuardianCommand.ResumeJob(id, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            if (Boolean.TRUE.equals(ok)) {
                ctx.status(202).json(Map.of("message", "Resume requested"));
            } else {
                ctx.status(404).json(Map.of("error", "Job not found"));
            }
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private void cancelJob(Context ctx) {
        String id = ctx.pathParam("id");
        try {
            Boolean ok = AskPattern.<ReplayGuardianCommand, Boolean>ask(guardian, (ActorRef<Boolean> replyTo) -> new ReplayGuardianCommand.CancelJob(id, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            if (Boolean.TRUE.equals(ok)) {
                ctx.status(202).json(Map.of("message", "Cancel requested"));
            } else {
                ctx.status(404).json(Map.of("error", "Job not found"));
            }
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private void getStatus(Context ctx) {
        String id = ctx.pathParam("id");
        try {
            ReplayJob job = AskPattern.<ReplayGuardianCommand, ReplayJob>ask(guardian, (ActorRef<ReplayJob> replyTo) -> new ReplayGuardianCommand.GetJob(id, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            if (job != null) {
                ctx.json(Map.of(
                        "job_id", job.getId(),
                        "status", job.getStatus().name(),
                        "error", job.getErrorMessage() != null ? job.getErrorMessage() : ""
                ));
            } else {
                ctx.status(404).json(Map.of("error", "Job not found"));
            }
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    private void getMetrics(Context ctx) {
        String id = ctx.pathParam("id");
        try {
            ReplayJob job = AskPattern.<ReplayGuardianCommand, ReplayJob>ask(guardian, (ActorRef<ReplayJob> replyTo) -> new ReplayGuardianCommand.GetJob(id, replyTo), askTimeout, system.scheduler())
                    .toCompletableFuture().get();
            if (job != null) {
                ctx.json(Map.of(
                        "job_id", job.getId(),
                        "events_read", job.getEventsRead(),
                        "events_sent", job.getEventsSent(),
                        "events_failed", job.getEventsFailed(),
                        "started_at", job.getStartedAt() != null ? job.getStartedAt().toString() : null,
                        "completed_at", job.getCompletedAt() != null ? job.getCompletedAt().toString() : null
                ));
            } else {
                ctx.status(404).json(Map.of("error", "Job not found"));
            }
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }
}
