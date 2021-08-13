package com.github.trask;

import java.time.Instant;
import java.util.List;

public class RunsResponse {

  public List<WorkflowRun> workflow_runs;

  public static class WorkflowRun {
    public long id;
    // re-running an action does not reset the created_at
    // which makes it useless for calculating queue time / build time
    public Instant created_at;

    public String url;
  }
}
