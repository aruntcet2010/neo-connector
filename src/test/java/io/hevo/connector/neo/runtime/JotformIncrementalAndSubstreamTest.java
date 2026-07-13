package io.hevo.connector.neo.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.neo.manifest.ManifestPipeline;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * P2 milestones on the REAL jotform manifest: the incremental `submissions` stream produces and
 * consumes cursor state, and the `questions` substream reads parent forms, fans out per form, and
 * applies its AddFields transformation.
 */
class JotformIncrementalAndSubstreamTest {

  private MockWebServer server;
  private SaasHttpClient client;

  @BeforeEach
  void setUp() throws Exception {
    server = new MockWebServer();
    server.start();
    client = SaasHttpClient.create();
  }

  @AfterEach
  void tearDown() throws Exception {
    client.close();
    server.shutdown();
  }

  private Map<String, Object> config() {
    return Map.of(
        "api_key", "k",
        "start_date", "2024-01-01T00:00:00Z",
        "end_date", "2024-03-01T00:00:00Z",
        "api_endpoint", Map.of("enterprise_url", server.url("/").toString().replaceAll("/$", "")));
  }

  private StreamSpec stream(String name) {
    ManifestPipeline.Manifest manifest =
        new ManifestPipeline().process(getClass().getResourceAsStream("/manifests/jotform.yaml"));
    return new StreamSpec(manifest.stream(name));
  }

  @Test
  void submissionsIncrementalStateRoundTrip() {
    server.setDispatcher(
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) {
            return new MockResponse()
                .setBody(
                    """
                                        {"content": [
                                          {"id": "s1", "created_at": "2024-01-10 09:00:00"},
                                          {"id": "s2", "created_at": "2024-02-15 10:30:00"}
                                        ]}
                                        """);
          }
        });

    ManifestPollTask task =
        new ManifestPollTask(
            null, CategoryType.HISTORICAL, stream("submissions"), client, config());

    List<Map<String, Object>> records = new ArrayList<>();
    ManifestPollTask.ReadResult first = task.read(records::add, null);
    assertEquals(2, first.recordCount());
    // State = highest created_at observed, legacy-CDK JSON shape.
    assertEquals("{\"created_at\":\"2024-02-15 10:30:00\"}", first.stateJson());

    // Second sync with that state: the window must start from the cursor, not start_date.
    ManifestPollTask task2 =
        new ManifestPollTask(
            null, CategoryType.HISTORICAL, stream("submissions"), client, config());
    ManifestPollTask.ReadResult second = task2.read(r -> {}, first.stateJson());
    assertNotNull(second.stateJson());
    assertEquals("{\"created_at\":\"2024-02-15 10:30:00\"}", second.stateJson());
  }

  @Test
  void questionsSubstreamFansOutPerParentFormAndAddsFormId() throws Exception {
    server.setDispatcher(
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) {
            String path = request.getPath();
            if (path.startsWith("/user/forms")) {
              return new MockResponse()
                  .setBody(
                      """
                                            {"content": [
                                              {"id": "F1", "title": "Form 1"},
                                              {"id": "F2", "title": "Form 2"}
                                            ]}
                                            """);
            }
            if (path.contains("/F1/questions")) {
              return new MockResponse()
                  .setBody("{\"content\": {\"q1\": {\"qid\": \"1\", \"text\":" + " \"Name?\"}}}");
            }
            if (path.contains("/F2/questions")) {
              return new MockResponse()
                  .setBody("{\"content\": {\"q1\": {\"qid\": \"9\", \"text\":" + " \"Email?\"}}}");
            }
            return new MockResponse().setResponseCode(404);
          }
        });

    ManifestPollTask task =
        new ManifestPollTask(null, CategoryType.HISTORICAL, stream("questions"), client, config());
    List<Map<String, Object>> records = new ArrayList<>();
    long count = task.read(records::add, null).recordCount();

    assertEquals(2, count);
    // AddFields stamped the parent form id onto each question record.
    Map<String, Object> first = records.get(0);
    assertEquals("1", first.get("qid"));
    assertEquals("F1", first.get("form_id"));
    assertEquals("F2", records.get(1).get("form_id"));

    // Requests: 1 parent read + 2 child reads with the partition in the path.
    List<String> paths = new ArrayList<>();
    RecordedRequest request;
    while ((request = server.takeRequest(100, java.util.concurrent.TimeUnit.MILLISECONDS))
        != null) {
      paths.add(request.getPath());
    }
    assertEquals(3, paths.size());
    assertTrue(paths.get(0).startsWith("/user/forms"), paths.get(0));
    assertTrue(paths.get(1).contains("/F1/questions"), paths.get(1));
    assertTrue(paths.get(2).contains("/F2/questions"), paths.get(2));
  }
}
