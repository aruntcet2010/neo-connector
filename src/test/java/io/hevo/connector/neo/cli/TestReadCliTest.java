package io.hevo.connector.neo.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** End-to-end tests of the harness-facing CLI: JSON report on stdout, evidence, limits. */
class TestReadCliTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @TempDir Path tmp;
  private MockWebServer server;

  @BeforeEach
  void setUp() throws Exception {
    server = new MockWebServer();
    server.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.shutdown();
  }

  private JsonNode runCli(String... args) throws Exception {
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    TestReadCli.run(args, new PrintStream(stdout, true, "UTF-8"));
    return JSON.readTree(stdout.toString("UTF-8"));
  }

  private Path writeManifest() throws Exception {
    String yaml =
        """
        version: 4.6.2
        type: DeclarativeSource
        check:
          type: CheckStream
          stream_names: [items]
        streams:
          - type: DeclarativeStream
            name: items
            primary_key: id
            retriever:
              type: SimpleRetriever
              requester:
                type: HttpRequester
                url_base: %s
                path: "/items"
                authenticator:
                  type: ApiKeyAuthenticator
                  api_token: "{{ config['api_key'] }}"
                  inject_into:
                    type: RequestOption
                    inject_into: request_parameter
                    field_name: apiKey
              record_selector:
                type: RecordSelector
                extractor:
                  type: DpathExtractor
                  field_path: ["content"]
              paginator:
                type: DefaultPaginator
                page_token_option:
                  type: RequestOption
                  inject_into: request_parameter
                  field_name: offset
                page_size_option:
                  type: RequestOption
                  inject_into: request_parameter
                  field_name: limit
                pagination_strategy:
                  type: OffsetIncrement
                  page_size: 2
            schema_loader:
              type: InlineSchemaLoader
              schema:
                type: object
                properties:
                  id: { type: integer }
        spec:
          type: Spec
          connection_specification:
            type: object
            properties: {}
        """
            .formatted(server.url("/").toString().replaceAll("/$", ""));
    Path path = tmp.resolve("manifest.yaml");
    Files.writeString(path, yaml);
    return path;
  }

  private Path writeConfig() throws Exception {
    Path path = tmp.resolve("config.json");
    Files.writeString(path, "{\"api_key\": \"sekret-value\"}");
    return path;
  }

  @Test
  void validateReportsSupportedStreams() throws Exception {
    JsonNode report = runCli("validate", "--manifest", writeManifest().toString());
    assertTrue(report.get("valid").asBoolean());
    assertTrue(report.get("all_streams_supported").asBoolean());
    JsonNode stream = report.get("streams").get(0);
    assertEquals("items", stream.get("name").asText());
    assertTrue(stream.get("supported").asBoolean());
    assertFalse(stream.get("incremental").asBoolean());
  }

  @Test
  void validateFlagsUnsupportedComponents() throws Exception {
    String yaml =
        """
        version: 4.6.2
        type: DeclarativeSource
        check: { type: CheckStream, stream_names: [things] }
        streams:
          - type: DeclarativeStream
            name: things
            retriever:
              type: AsyncRetriever
              status_mapping: { running: [], completed: [], failed: [], timeout: [] }
              status_extractor:
                type: DpathExtractor
                field_path: ["status"]
              download_target_extractor:
                type: DpathExtractor
                field_path: ["url"]
              creation_requester:
                type: HttpRequester
                url_base: https://x.test
              polling_requester:
                type: HttpRequester
                url_base: https://x.test
              download_requester:
                type: HttpRequester
                url_base: https://x.test
              record_selector:
                type: RecordSelector
                extractor: { type: DpathExtractor, field_path: [] }
        spec:
          type: Spec
          connection_specification: { type: object, properties: {} }
        """;
    Path path = tmp.resolve("async.yaml");
    Files.writeString(path, yaml);
    JsonNode report = runCli("validate", "--manifest", path.toString());
    assertTrue(report.get("valid").asBoolean());
    assertFalse(report.get("all_streams_supported").asBoolean());
    JsonNode stream = report.get("streams").get(0);
    assertFalse(stream.get("supported").asBoolean());
    assertTrue(stream.get("reason").asText().contains("retriever"));
  }

  @Test
  void validateConstructsFullRuntimeGraph() throws Exception {
    // OAuth is schema-valid but the engine can't execute it: gate 2 must catch it at
    // validate time, not first read. (Regression: validate once checked StreamSpec only.)
    String yaml =
        """
        version: 4.6.2
        type: DeclarativeSource
        check: { type: CheckStream, stream_names: [things] }
        streams:
          - type: DeclarativeStream
            name: things
            retriever:
              type: SimpleRetriever
              requester:
                type: HttpRequester
                url_base: https://x.test
                authenticator:
                  type: OAuthAuthenticator
                  token_refresh_endpoint: https://x.test/token
                  client_id: "{{ config['client_id'] }}"
                  client_secret: "{{ config['client_secret'] }}"
                  refresh_token: "{{ config['refresh_token'] }}"
              record_selector:
                type: RecordSelector
                extractor: { type: DpathExtractor, field_path: [] }
        spec:
          type: Spec
          connection_specification: { type: object, properties: {} }
        """;
    Path path = tmp.resolve("oauth.yaml");
    Files.writeString(path, yaml);
    JsonNode report = runCli("validate", "--manifest", path.toString());
    assertTrue(report.get("valid").asBoolean());
    assertFalse(report.get("all_streams_supported").asBoolean());
    JsonNode stream = report.get("streams").get(0);
    assertFalse(stream.get("supported").asBoolean());
    assertTrue(
        stream.get("reason").asText().toLowerCase().contains("authenticator"),
        stream.get("reason").asText());
  }

  @Test
  void readProducesEvidenceWithRedactedSecrets() throws Exception {
    server.enqueue(
        new MockResponse().setBody("{\"content\": [{\"id\": 1}, {\"id\": 2}]}"));
    server.enqueue(new MockResponse().setBody("{\"content\": [{\"id\": 3}]}"));

    JsonNode report =
        runCli(
            "read",
            "--manifest", writeManifest().toString(),
            "--stream", "items",
            "--config-file", writeConfig().toString());

    assertTrue(report.get("success").asBoolean());
    assertEquals(3, report.get("record_count").asInt());
    assertEquals(2, report.get("records_sample").get(1).get("id").asInt());
    assertTrue(report.get("state_after").isNull());

    JsonNode pages = report.get("slices").get(0).get("pages");
    assertEquals(2, pages.size());
    String url = pages.get(0).get("request").get("url").asText();
    assertTrue(url.contains("apiKey=****"), url);
    assertFalse(report.toString().contains("sekret-value"), "secret leaked into report");
    assertEquals(200, pages.get(0).get("response").get("status").asInt());
    assertEquals(2, pages.get(0).get("record_count").asInt());
    assertTrue(pages.get(1).get("request").get("url").asText().contains("offset=2"));
  }

  @Test
  void readHonorsLimits() throws Exception {
    // Every page is full (2 records), so pagination would continue forever.
    server.setDispatcher(
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) {
            return new MockResponse().setBody("{\"content\": [{\"id\": 1}, {\"id\": 2}]}");
          }
        });
    JsonNode report =
        runCli(
            "read",
            "--manifest", writeManifest().toString(),
            "--stream", "items",
            "--config-file", writeConfig().toString(),
            "--max-pages", "2",
            "--max-records", "10");
    assertTrue(report.get("limit_reached").asBoolean());
    assertEquals(4, report.get("record_count").asInt()); // 2 pages × 2 records
    assertEquals(2, report.get("slices").get(0).get("pages").size());
  }

  @Test
  void readPageSizeOverride() throws Exception {
    server.enqueue(new MockResponse().setBody("{\"content\": []}"));
    JsonNode report =
        runCli(
            "read",
            "--manifest", writeManifest().toString(),
            "--stream", "items",
            "--config-file", writeConfig().toString(),
            "--page-size", "1");
    assertEquals("applied", report.get("page_size_override").asText());
    assertTrue(
        report.get("slices").get(0).get("pages").get(0).get("request").get("url").asText()
            .contains("limit=1"));
  }

  @Test
  void httpErrorProducesErrorReportNotCrash() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(401).setBody("{\"error\": \"nope\"}"));
    JsonNode report =
        runCli(
            "read",
            "--manifest", writeManifest().toString(),
            "--stream", "items",
            "--config-file", writeConfig().toString());
    assertFalse(report.get("success").asBoolean());
    assertEquals(1, report.get("errors").size());
    // The failing page is still evidence: status 401 visible to the agent.
    assertEquals(
        401,
        report.get("slices").get(0).get("pages").get(0).get("response").get("status").asInt());
  }

  @Test
  void unknownStreamIsCleanJsonError() throws Exception {
    JsonNode report =
        runCli(
            "read",
            "--manifest", writeManifest().toString(),
            "--stream", "nope",
            "--config-file", writeConfig().toString());
    assertFalse(report.get("success").asBoolean());
    assertNotNull(report.get("errors").get(0).get("message"));
    assertNull(report.get("stream"));
  }
}
