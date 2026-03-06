package com.drdemo.http;

import com.drdemo.actors.messages.JobCommand;
import com.drdemo.actors.messages.JobResponse;
import com.drdemo.model.CreateJobRequest;
import com.drdemo.model.ReplayJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.typesafe.config.Config;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Pekko HTTP API server exposing all replay job management endpoints.
 * Uses Ask pattern to communicate with the JobSupervisorActor.
 */
public class ApiServer extends AllDirectives {

    private static final Logger log = LoggerFactory.getLogger(ApiServer.class);
    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(10);

    private final ActorSystem<JobCommand> system;
    private final Config config;
    private final ObjectMapper mapper;

    // Prometheus-style counters (in-memory for demo)
    private long totalJobsCreated = 0;
    private long totalRequests = 0;
    private final long startTime = System.currentTimeMillis();

    public ApiServer(ActorSystem<JobCommand> system, Config config) {
        this.system = system;
        this.config = config;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public void start() {
        String host = config.getString("dr-replay.http.host");
        int port = config.getInt("dr-replay.http.port");

        Http.get(system)
                .newServerAt(host, port)
                .bind(buildRoutes());
    }

    private Route buildRoutes() {
        return concat(
                // Health check
                path("health", () -> get(() -> handleHealth())),

                // Prometheus metrics
                path("metrics", () -> get(() -> handleMetrics())),

                // API v1 routes
                pathPrefix("api", () ->
                        pathPrefix("v1", () ->
                                pathPrefix("replay", () ->
                                        pathPrefix("jobs", () -> concat(

                                                // POST /api/v1/replay/jobs — create job
                                                pathEndOrSingleSlash(() -> concat(
                                                        post(() -> entity(
                                                                org.apache.pekko.http.javadsl.unmarshalling.Unmarshaller.entityToString(),
                                                                body -> handleCreateJob(body)
                                                        )),
                                                        // GET /api/v1/replay/jobs — list all
                                                        get(() -> handleListJobs())
                                                )),

                                                // /api/v1/replay/jobs/{id}/*
                                                pathPrefix(segment -> concat(
                                                        // GET /api/v1/replay/jobs/{id}
                                                        pathEndOrSingleSlash(() -> get(() -> handleGetJob(segment))),

                                                        // POST /api/v1/replay/jobs/{id}/start
                                                        path("start", () -> post(() -> handleStartJob(segment))),

                                                        // POST /api/v1/replay/jobs/{id}/pause
                                                        path("pause", () -> post(() -> handlePauseJob(segment))),

                                                        // POST /api/v1/replay/jobs/{id}/resume
                                                        path("resume", () -> post(() -> handleResumeJob(segment))),

                                                        // POST /api/v1/replay/jobs/{id}/cancel
                                                        path("cancel", () -> post(() -> handleCancelJob(segment))),

                                                        // GET /api/v1/replay/jobs/{id}/status
                                                        path("status", () -> get(() -> handleGetJobStatus(segment))),

                                                        // GET /api/v1/replay/jobs/{id}/metrics
                                                        path("metrics", () -> get(() -> handleGetJobMetrics(segment)))
                                                ))
                                        ))
                                )
                        )
                )
        );
    }

    // ---- Handler methods ----

    private Route handleHealth() {
        totalRequests++;
        try {
            Map<String, Object> health = Map.of(
                    "status", "UP",
                    "service", "dr-replay-service",
                    "timestamp", Instant.now().toString(),
                    "uptime_seconds", (System.currentTimeMillis() - startTime) / 1000
            );
            return complete(HttpResponse.create()
                    .withStatus(StatusCodes.OK)
                    .withEntity(ContentTypes.APPLICATION_JSON, toJson(health)));
        } catch (Exception e) {
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Health check failed: " + e.getMessage());
        }
    }

    private Route handleMetrics() {
        totalRequests++;
        // Prometheus text format
        StringBuilder sb = new StringBuilder();
        sb.append("# HELP dr_replay_jobs_created_total Total replay jobs created\n");
        sb.append("# TYPE dr_replay_jobs_created_total counter\n");
        sb.append("dr_replay_jobs_created_total ").append(totalJobsCreated).append("\n");
        sb.append("# HELP dr_replay_http_requests_total Total HTTP requests\n");
        sb.append("# TYPE dr_replay_http_requests_total counter\n");
        sb.append("dr_replay_http_requests_total ").append(totalRequests).append("\n");
        sb.append("# HELP dr_replay_uptime_seconds Service uptime in seconds\n");
        sb.append("# TYPE dr_replay_uptime_seconds gauge\n");
        sb.append("dr_replay_uptime_seconds ").append((System.currentTimeMillis() - startTime) / 1000).append("\n");
        return complete(HttpResponse.create()
                .withStatus(StatusCodes.OK)
                .withEntity(org.apache.pekko.http.javadsl.model.MediaTypes.TEXT_PLAIN.toContentType(
                        org.apache.pekko.http.javadsl.model.HttpCharsets.UTF_8), sb.toString()));
    }

    private Route handleCreateJob(String body) {
        totalRequests++;
        try {
            CreateJobRequest req = mapper.readValue(body, CreateJobRequest.class);

            ReplayJob job = new ReplayJob();
            job.setName(req.getName() != null ? req.getName() : "Replay-" + System.currentTimeMillis());
            job.setDescription(req.getDescription());
            job.setDestinationType(req.getDestinationType() != null
                    ? req.getDestinationType() : ReplayJob.DestinationType.BOTH);
            job.setKafkaTopic(req.getKafkaTopic());
            job.setRestEndpoint(req.getRestEndpoint());
            job.setStartTime(req.getStartTime());
            job.setEndTime(req.getEndTime());
            job.setCustomerIds(req.getCustomerIds());
            job.setEventTypes(req.getEventTypes());
            if (req.getRatePerSecond() > 0) job.setRatePerSecond(req.getRatePerSecond());
            if (req.getBatchSize() > 0) job.setBatchSize(req.getBatchSize());

            CompletionStage<JobResponse> response = AskPattern.ask(
                    system,
                    replyTo -> new JobCommand.CreateJob(job, replyTo),
                    ASK_TIMEOUT,
                    system.scheduler()
            );

            return onSuccess(response, resp -> {
                if (resp instanceof JobResponse.JobCreated created) {
                    totalJobsCreated++;
                    return complete(HttpResponse.create()
                            .withStatus(StatusCodes.CREATED)
                            .withEntity(ContentTypes.APPLICATION_JSON, toJson(created.job())));
                }
                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
            });
        } catch (Exception e) {
            log.error("Failed to parse create job request", e);
            return complete(StatusCodes.BAD_REQUEST, "Invalid request body: " + e.getMessage());
        }
    }

    private Route handleListJobs() {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.ListJobs(replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.JobList list) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON, toJson(list.jobs())));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route handleGetJob(String jobId) {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.GetJobDetails(jobId, replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.JobDetails details) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON, toJson(details.job())));
            } else if (resp instanceof JobResponse.JobNotFound) {
                return complete(StatusCodes.NOT_FOUND, notFoundJson(jobId));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route handleStartJob(String jobId) {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.StartJob(jobId, replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.JobStarted) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON,
                                toJson(Map.of("status", "started", "job_id", jobId))));
            } else if (resp instanceof JobResponse.JobNotFound) {
                return complete(StatusCodes.NOT_FOUND, notFoundJson(jobId));
            } else if (resp instanceof JobResponse.JobError err) {
                return complete(StatusCodes.CONFLICT,
                        toJson(Map.of("error", err.message(), "job_id", jobId)));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route handlePauseJob(String jobId) {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.PauseJob(jobId, replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.JobPaused) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON,
                                toJson(Map.of("status", "paused", "job_id", jobId))));
            } else if (resp instanceof JobResponse.JobNotFound) {
                return complete(StatusCodes.NOT_FOUND, notFoundJson(jobId));
            } else if (resp instanceof JobResponse.JobError err) {
                return complete(StatusCodes.CONFLICT,
                        toJson(Map.of("error", err.message(), "job_id", jobId)));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route handleResumeJob(String jobId) {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.ResumeJob(jobId, replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.JobResumed) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON,
                                toJson(Map.of("status", "resumed", "job_id", jobId))));
            } else if (resp instanceof JobResponse.JobNotFound) {
                return complete(StatusCodes.NOT_FOUND, notFoundJson(jobId));
            } else if (resp instanceof JobResponse.JobError err) {
                return complete(StatusCodes.CONFLICT,
                        toJson(Map.of("error", err.message(), "job_id", jobId)));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route handleCancelJob(String jobId) {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.CancelJob(jobId, replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.JobCancelled) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON,
                                toJson(Map.of("status", "cancelled", "job_id", jobId))));
            } else if (resp instanceof JobResponse.JobNotFound) {
                return complete(StatusCodes.NOT_FOUND, notFoundJson(jobId));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route handleGetJobStatus(String jobId) {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.GetJobStatus(jobId, replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.JobStatus status) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON, toJson(status.job())));
            } else if (resp instanceof JobResponse.JobNotFound) {
                return complete(StatusCodes.NOT_FOUND, notFoundJson(jobId));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route handleGetJobMetrics(String jobId) {
        totalRequests++;
        CompletionStage<JobResponse> response = AskPattern.ask(
                system,
                replyTo -> new JobCommand.GetJobMetrics(jobId, replyTo),
                ASK_TIMEOUT,
                system.scheduler()
        );
        return onSuccess(response, resp -> {
            if (resp instanceof JobResponse.MetricsSnapshot snapshot) {
                return complete(HttpResponse.create()
                        .withStatus(StatusCodes.OK)
                        .withEntity(ContentTypes.APPLICATION_JSON, toJson(snapshot.metrics())));
            } else if (resp instanceof JobResponse.JobNotFound) {
                return complete(StatusCodes.NOT_FOUND, notFoundJson(jobId));
            }
            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    // ---- Helpers ----

    private String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("JSON serialization error", e);
            return "{\"error\":\"serialization failed\"}";
        }
    }

    private String notFoundJson(String jobId) {
        return toJson(Map.of("error", "Job not found", "job_id", jobId));
    }
}
