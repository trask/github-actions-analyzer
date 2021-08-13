package com.github.trask;

import java.time.Instant;
import java.util.List;

public class JobsResponse {

  public int total_count;

  public List<Job> jobs;

  public static class Job {
    public long id;
    public String status;
    public Instant started_at;
    public Instant completed_at;

    public String url;
  }
}
