package com.drdemo.actors.messages;

import com.drdemo.model.JobMetrics;
import com.drdemo.model.ReplayJob;

import java.io.Serializable;
import java.util.List;

/**
 * Response types returned by actors to API handlers.
 */
public interface JobResponse extends Serializable {

    record JobCreated(ReplayJob job) implements JobResponse {}
    record JobStarted(String jobId) implements JobResponse {}
    record JobPaused(String jobId) implements JobResponse {}
    record JobResumed(String jobId) implements JobResponse {}
    record JobCancelled(String jobId) implements JobResponse {}
    record JobStatus(ReplayJob job) implements JobResponse {}
    record JobDetails(ReplayJob job) implements JobResponse {}
    record MetricsSnapshot(JobMetrics metrics) implements JobResponse {}
    record JobList(List<ReplayJob> jobs) implements JobResponse {}
    record JobError(String jobId, String message) implements JobResponse {}
    record JobNotFound(String jobId) implements JobResponse {}
}
