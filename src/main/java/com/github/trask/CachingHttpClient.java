package com.github.trask;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.OptionalLong;

import static java.util.concurrent.TimeUnit.SECONDS;

class CachingHttpClient {

  private static final String AUTH_TOKEN = System.getenv("GITHUB_AUTH_TOKEN");

  private static final boolean BYPASS_ETAG_CHECK = false;
  private static final boolean PRINT_PROGRESS = false;

  private static final ObjectMapper mapper;

  private static final HttpClient httpClient = HttpClient.newHttpClient();

  static {
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.findAndRegisterModules();
  }

  private final Connection connection;
  private final PreparedStatement read;
  private final PreparedStatement insert;
  private final PreparedStatement update;

  CachingHttpClient() throws SQLException {
    connection = DriverManager.getConnection("jdbc:h2:./cache");
    try (var statement = connection.createStatement()) {
      statement.execute(
          "create table if not exists cache (uri varchar, body varchar, etag varchar)");
      statement.execute("create index if not exists cache_index on cache (uri)");
    }
    read = connection.prepareStatement("select body, etag from cache where uri = ?");
    insert = connection.prepareStatement("insert into cache (uri, body, etag) values (?, ?, ?)");
    update = connection.prepareStatement("update cache set body = ?, etag = ? where uri = ?");
  }

  void close() throws SQLException {
    connection.close();
  }

  <T> T get(String uri, Class<T> type) throws Exception {
    return mapper.readValue(internalGet(uri), type);
  }

  <T> T get(String uri, TypeReference<T> type) throws Exception {
    return mapper.readValue(internalGet(uri), type);
  }

  private String internalGet(String uri) throws Exception {
    String cachedBody = null;
    String cachedEtag = null;

    synchronized (connection) {
      read.setString(1, uri);
      try (var results = read.executeQuery()) {
        if (results.next()) {
          cachedBody = results.getString(1);
          cachedEtag = results.getString(2);
        }
      }
    }

    if (cachedBody != null && cachedEtag != null) {
      if (BYPASS_ETAG_CHECK) {
        if (PRINT_PROGRESS) {
          System.out.println("bypass etag check: " + uri);
        }
        return cachedBody;
      }
      var request =
          HttpRequest.newBuilder()
              .uri(URI.create(uri))
              .method("GET", HttpRequest.BodyPublishers.noBody())
              .header("Authorization", "token " + AUTH_TOKEN)
              .header("If-None-Match", cachedEtag)
              .build();
      var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 304) {
        printProgress("found in cache", response);
        return cachedBody;
      } else if (response.statusCode() == 200) {
        String updatedBody = response.body();
        String updatedEtag = response.headers().firstValue("ETag").orElse(null);
        if (updatedEtag != null) {
          synchronized (connection) {
            update.setString(1, updatedBody);
            update.setString(2, updatedEtag);
            update.setString(3, uri);
            update.execute();
          }
        }
        printProgress("found in cache, but since updated", response);
        return updatedBody;
      } else {
        return retryIfPossible(response, uri);
      }
    }

    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .method("GET", HttpRequest.BodyPublishers.noBody())
            .header("Authorization", "token " + AUTH_TOKEN)
            .build();
    var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      return retryIfPossible(response, uri);
    }
    String body = response.body();
    String etag = response.headers().firstValue("ETag").orElse(null);
    if (etag != null) {
      synchronized (connection) {
        insert.setString(1, uri);
        insert.setString(2, body);
        insert.setString(3, etag);
        insert.execute();
      }
      printProgress("stored in cache", response);
    } else {
      printProgress("no etag", response);
    }
    return response.body();
  }

  private String retryIfPossible(HttpResponse<String> response, String uri) throws Exception {
    OptionalLong retryAfter = response.headers().firstValueAsLong("retry-after");
    if (retryAfter.isPresent()) {
      // sleep a bit and try again
      System.out.println("retry-after, sleeping for " + retryAfter.getAsLong() + " seconds");
      SECONDS.sleep(retryAfter.getAsLong());
      return internalGet(uri);
    }
    OptionalLong ratelimitReset = response.headers().firstValueAsLong("x-ratelimit-reset");
    if (ratelimitReset.isPresent()) {
      // sleep a bit and try again
      long sleepSeconds = ratelimitReset.getAsLong() - System.currentTimeMillis() / 1000;
      System.out.println(
          "x-ratelimit-reset, sleeping until "
              + LocalTime.ofInstant(
                  Instant.ofEpochSecond(ratelimitReset.getAsLong()), ZoneId.systemDefault()));
      SECONDS.sleep(sleepSeconds);
      return internalGet(uri);
    }
    throw new IllegalStateException(
        "Unexpected response "
            + response.statusCode()
            + ": "
            + response.uri()
            + "\n"
            + response.headers()
            + "\n"
            + response.body());
  }

  private static void printProgress(String message, HttpResponse<?> response) {
    if (PRINT_PROGRESS) {
      System.out.println(
          message
              + ": "
              + response.uri()
              + ", x-ratelimit-remaining: "
              + response.headers().firstValue("x-ratelimit-remaining").orElse("n/a"));
    }
  }
}
