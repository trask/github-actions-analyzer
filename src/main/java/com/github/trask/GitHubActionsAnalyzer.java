package com.github.trask;

import com.fasterxml.jackson.core.type.TypeReference;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class GitHubActionsAnalyzer {

  private static final LocalDate START_DATE = LocalDate.of(2023, 1, 23);
  private static final ZoneId TIME_ZONE = ZoneId.of("America/Los_Angeles");

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
      var totalDurationsByDay = new HashMap<LocalDate, Set<WorkflowData>>();

      for (var repo : getRepos()) {
        System.out.println(repo);
        Map<LocalDate, Set<WorkflowData>> workflows = getWorkflowData(repo);
        print(workflows);
        System.out.println();

        for (var entry : workflows.entrySet()) {
          totalDurationsByDay
              .computeIfAbsent(entry.getKey(), d -> new HashSet<>())
              .addAll(entry.getValue());
        }
      }

      System.out.println("totals");
      print(totalDurationsByDay);
      System.out.println();
    } finally {
      httpClient.close();
      executor.shutdown();
    }
  }

  private static Map<LocalDate, Set<WorkflowData>> getWorkflowData(String repo) throws Exception {
    var workflowRuns = getWorkflowRuns(repo);

    var durationsByDay = new HashMap<LocalDate, Set<WorkflowData>>();

    var futures = new ArrayList<Future<Optional<WorkflowData>>>();
    for (var workflowRun : workflowRuns) {
      futures.add(
          executor.submit(
              () -> {
                var jobs = getJobs(repo, workflowRun);
                var totalUsage = Duration.ZERO;
                Instant firstJobStartedAt = null;
                for (var job : jobs) {
                  if (job.started_at == null) {
                    // job is queued and hasn't started
                    continue;
                  }
                  if (job.completed_at != null) {
                    if (firstJobStartedAt == null || job.started_at.isBefore(firstJobStartedAt)) {
                      firstJobStartedAt = job.started_at;
                    }
                    var duration = Duration.between(job.started_at, job.completed_at);
                    totalUsage = totalUsage.plus(duration);
                  }
                }
                if (firstJobStartedAt == null) {
                  return Optional.empty();
                }
                var date = LocalDate.ofInstant(workflowRun.created_at, TIME_ZONE);
                return Optional.of(new WorkflowData(date, totalUsage));
              }));
    }

    for (var future : futures) {
      future
          .get()
          .ifPresent(
              workflowData ->
                  durationsByDay
                      .computeIfAbsent(workflowData.date, d -> new HashSet<>())
                      .add(workflowData));
    }

    return durationsByDay;
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
    var workflowRuns = new ArrayList<RunsResponse.WorkflowRun>();
    int page = 1;
    while (true) {
      var runsResponse =
          sendRepoRequest(repo, "/actions/runs?per_page=100&page=" + page, RunsResponse.class);
      if (runsResponse.workflow_runs.isEmpty()) {
        return workflowRuns;
      }
      for (RunsResponse.WorkflowRun workflowRun : runsResponse.workflow_runs) {
        if (LocalDate.ofInstant(workflowRun.created_at, TIME_ZONE).isBefore(START_DATE)) {
          return workflowRuns;
        }
        workflowRuns.add(workflowRun);
      }
      page++;
    }
  }

  private static List<JobsResponse.Job> getJobs(String repo, RunsResponse.WorkflowRun workflowRun)
      throws Exception {
    var jobsResponse =
        sendRepoRequest(
            repo, "/actions/runs/" + workflowRun.id + "/jobs?per_page=1", JobsResponse.class);
    var jobs = new ArrayList<JobsResponse.Job>();
    int i = 1;
    while (jobs.size() < jobsResponse.total_count) {
      jobsResponse =
          sendRepoRequest(
              repo,
              "/actions/runs/" + workflowRun.id + "/jobs?per_page=100&page=" + i,
              JobsResponse.class);
      jobs.addAll(jobsResponse.jobs);
      i++;
    }
    return jobs;
  }

  private static <T> T sendRepoRequest(String repo, String path, Class<T> type) throws Exception {
    return httpClient.get("https://api.github.com/repos/open-telemetry/" + repo + path, type);
  }

  private static void print(Map<LocalDate, Set<WorkflowData>> durationsByDay) {
    durationsByDay.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .filter(entry -> entry.getKey().getDayOfWeek().getValue() < 6) // week days
        .forEach(
            entry -> {
              Set<WorkflowData> workflows = entry.getValue();
              long totalUsage =
                  workflows.stream().mapToLong(workflow -> workflow.totalUsage.toMinutes()).sum();
              System.out.printf(
                  "%s - %4d builds, total usage: %6.1f hours%n",
                  entry.getKey(), workflows.size(), totalUsage / 60.0);
            });
  }

  static class WorkflowData {
    final LocalDate date;
    final Duration totalUsage;

    WorkflowData(LocalDate date, Duration totalUsage) {
      this.date = date;
      this.totalUsage = totalUsage;
    }
  }
}
