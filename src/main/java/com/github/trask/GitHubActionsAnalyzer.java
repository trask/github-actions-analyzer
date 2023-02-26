package com.github.trask;

import com.fasterxml.jackson.core.type.TypeReference;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class GitHubActionsAnalyzer {

  private static final Instant START = Instant.parse("2023-01-01T00:00:00Z");
  private static final Instant END = Instant.parse("2023-03-01T00:00:00Z");

  private static final int PAGE_SIZE = 100; // this is the largest possible page size

  private static final Duration MIN_QUEUE_TIME = Duration.ofMinutes(30);

  private static final Duration WORKFLOW_TIMEOUT = Duration.ofHours(6);

  private static final ExecutorService executor = Executors.newFixedThreadPool(16);
  private static final CachingHttpClient httpClient;

  static {
    try {
      httpClient = new CachingHttpClient();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      for (var repo : getRepos()) {
        System.out.println(repo);
        getWorkflowData(repo);
      }
    } finally {
      httpClient.close();
      executor.shutdown();
    }
  }

  private static void getWorkflowData(String repo) throws Exception {
    var workflowRuns = getWorkflowRuns(repo);

    for (var workflowRun : workflowRuns) {
      if (workflowRun.run_attempt > 1) {
        // created_at is not reset on retries, so altDuration below is not useful
        continue;
      }
      if (workflowRun.getDuration().compareTo(MIN_QUEUE_TIME) < 0
          || workflowRun.getDuration().compareTo(WORKFLOW_TIMEOUT) > 0) {
        continue;
      }
      var altDuration =
          workflowRun.updated_at.toEpochMilli() - workflowRun.created_at.toEpochMilli();
      altDuration /= 60000;
      if (altDuration > 60 && altDuration < 360) { // 360+ probably means issue with test timeout

        List<JobsResponse.Job> jobs = getJobs(repo, workflowRun);

        Instant firstJobStartedAt =
            jobs.stream().map(job -> job.started_at).min(Comparator.naturalOrder()).orElse(null);

        Duration queueDuration;
        if (firstJobStartedAt != null) {
          queueDuration = Duration.between(workflowRun.created_at, firstJobStartedAt);
        } else {
          // there were no jobs run (e.g. all jobs were skipped), so essentially the whole run was
          // queue time
          queueDuration = workflowRun.getDuration();
        }

        if (queueDuration.compareTo(MIN_QUEUE_TIME) > 0) {
          System.out.println(
              workflowRun.created_at
                  + " -- "
                  + queueDuration.toMinutes()
                  + " -- "
                  + workflowRun.getDuration().toMinutes()
                  + " -- "
                  + workflowRun.html_url);
        }
      }
    }
  }

  private static List<String> getRepos() throws Exception {
    var response =
        httpClient.get(
            "https://api.github.com/orgs/open-telemetry/repos",
            new TypeReference<List<RepoResponse>>() {});
    return response.stream()
        .map(repo -> repo.name)
        .sorted(String::compareTo)
        .collect(Collectors.toList());
  }

  private static List<RunsResponse.WorkflowRun> getWorkflowRuns(String repo) throws Exception {
    return getWorkflowRuns(repo, START, END);
  }

  private static List<RunsResponse.WorkflowRun> getWorkflowRuns(
      String repo, Instant start, Instant end) throws Exception {

    var runsResponse = getRunsResponse(repo, start, end, 1);

    if (runsResponse.total_count > 1000) {
      // need to reduce time range since github limit search results to 1000 (even across
      // pagination)

      // splitting by days/hours/minutes/seconds (instead of into arbitray intervals)
      // in order to improve cacheability of the results
      var difference = Duration.between(start, end);
      Duration interval;
      if (difference.toHours() > 24) {
        // split by days
        interval = Duration.ofDays(1);
      } else if (difference.toMinutes() > 60) {
        // split by hours
        interval = Duration.ofHours(1);
      } else if (difference.toSeconds() > 60) {
        // split by minutes
        interval = Duration.ofMinutes(1);
      } else {
        // split by seconds
        interval = Duration.ofSeconds(1);
      }

      var futures = new ArrayList<Future<List<RunsResponse.WorkflowRun>>>();

      var from = start;
      var to = start.plus(interval);
      while (!to.isAfter(end)) {
        Instant finalFrom = from;
        Instant finalTo = to;
        futures.add(executor.submit(() -> getWorkflowRuns(repo, finalFrom, finalTo)));
        from = to;
        to = to.plus(interval);
      }

      var results = new ArrayList<RunsResponse.WorkflowRun>();
      for (Future<List<RunsResponse.WorkflowRun>> future : futures) {
        results.addAll(future.get());
      }
      return results;
    }

    var results = new ArrayList<>(runsResponse.workflow_runs);
    for (int page = 2; page <= Math.ceil(runsResponse.total_count / (double) PAGE_SIZE); page++) {
      results.addAll(getRunsResponse(repo, start, end, page).workflow_runs);
    }

    if (results.size() != runsResponse.total_count) {
      System.out.println("INCORRECT NUMBER OF RESULTS");
      System.out.println(results.size());
      System.out.println(runsResponse.total_count);
      System.exit(1);
    }

    results.sort(Comparator.comparing(job -> job.created_at));
    return results;
  }

  private static RunsResponse getRunsResponse(String repo, Instant from, Instant to, int page)
      throws Exception {
    return sendRepoRequest(
        repo,
        "/actions/runs?created="
            + from
            + ".."
            + to
            + "&event=pull_request&per_page="
            + PAGE_SIZE
            + "&page="
            + page
            + "&exclude_pull_requests=true",
        RunsResponse.class);
  }

  private static List<JobsResponse.Job> getJobs(String repo, RunsResponse.WorkflowRun workflowRun)
      throws Exception {
    var jobsResponse = getJobsResponse(repo, workflowRun.id, 1);

    var results = new ArrayList<>(jobsResponse.jobs);
    for (int page = 2; page <= Math.ceil(jobsResponse.total_count / (double) PAGE_SIZE); page++) {
      results.addAll(getJobsResponse(repo, workflowRun.id, page).jobs);
    }

    if (results.size() != jobsResponse.total_count) {
      System.out.println("INCORRECT NUMBER OF RESULTS");
      System.out.println(results.size());
      System.out.println(jobsResponse.total_count);
      System.exit(1);
    }
    return results;
  }

  private static JobsResponse getJobsResponse(String repo, long workflowRunId, int page)
      throws Exception {
    return sendRepoRequest(
        repo,
        "/actions/runs/" + workflowRunId + "/jobs?per_page=" + PAGE_SIZE + "&page=" + page,
        JobsResponse.class);
  }

  private static <T> T sendRepoRequest(String repo, String path, Class<T> type) throws Exception {
    return httpClient.get("https://api.github.com/repos/open-telemetry/" + repo + path, type);
  }
}
