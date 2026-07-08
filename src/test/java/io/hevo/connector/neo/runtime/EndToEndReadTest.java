package io.hevo.connector.neo.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.neo.manifest.ManifestPipeline;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Full-path test: manifest YAML → pipeline → StreamSpec → ManifestPollTask read loop against a
 * mock HTTP API, asserting both the emitted records and the outbound request shapes (auth
 * injection, pagination parameters).
 */
class EndToEndReadTest {

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

    private String manifest() {
        return """
                version: 4.6.2
                type: DeclarativeSource
                check:
                  type: CheckStream
                  stream_names:
                    - items
                streams:
                  - type: DeclarativeStream
                    name: items
                    primary_key: id
                    retriever:
                      type: SimpleRetriever
                      requester:
                        type: HttpRequester
                        url_base: %s
                        path: "/v1/items"
                        http_method: GET
                        request_parameters:
                          workspace: "{{ config['workspace'] }}"
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
                          id:
                            type: integer
                          name:
                            type:
                              - "null"
                              - string
                          created_at:
                            type: string
                            format: date-time
                          settings:
                            type: object
                spec:
                  type: Spec
                  connection_specification:
                    type: object
                    properties:
                      api_key:
                        type: string
                """
                .formatted(server.url("/").toString().replaceAll("/$", ""));
    }

    private StreamSpec itemsStream() {
        ManifestPipeline.Manifest parsed = new ManifestPipeline().process(manifest());
        return new StreamSpec(parsed.stream("items"));
    }

    @Test
    void readsAllPagesWithAuthAndPagination() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"content\": [{\"id\": 1, \"name\": \"a\"}, {\"id\": 2,"
                                        + " \"name\": \"b\"}]}"));
        server.enqueue(new MockResponse().setBody("{\"content\": [{\"id\": 3}]}"));

        Map<String, Object> config = Map.of("api_key", "secret-token", "workspace", "w1");
        ManifestPollTask task =
                new ManifestPollTask(null, CategoryType.HISTORICAL, itemsStream(), client, config);

        List<Object> records = new ArrayList<>();
        long count = task.readStream(records::add);

        assertEquals(3, count);
        assertEquals(Map.of("id", 3), records.get(2));

        // First request: auth + config interpolation + page size, no offset yet.
        RecordedRequest first = server.takeRequest();
        assertEquals("/v1/items?workspace=w1&apiKey=secret-token&limit=2", first.getPath());

        // Second request: offset token injected after a full page.
        RecordedRequest second = server.takeRequest();
        assertTrue(second.getPath().contains("offset=2"), second.getPath());
        assertTrue(second.getPath().contains("apiKey=secret-token"), second.getPath());
    }

    @Test
    void schemaSynthesisFromInlineSchema() {
        ObjectSchema schema = SchemaSynthesizer.synthesize(itemsStream());
        assertEquals("items", schema.objectDetail().getTableFullyQualifiedName());
        assertEquals(4, schema.fields().size());
        var byName =
                schema.fields().stream()
                        .collect(java.util.stream.Collectors.toMap(f -> f.name(), f -> f));
        assertNotNull(byName.get("id"));
        assertEquals("hudt_long", byName.get("id").logicalType());
        assertEquals("hudt_varchar", byName.get("name").logicalType());
        assertEquals("hudt_date_time_tz", byName.get("created_at").logicalType());
        assertEquals("hudt_json", byName.get("settings").logicalType());
    }

    @Test
    void bearerAuthGoesToHeader() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"content\": []}"));
        String yaml =
                """
                version: 4.6.2
                type: DeclarativeSource
                check:
                  type: CheckStream
                  stream_names: [things]
                streams:
                  - type: DeclarativeStream
                    name: things
                    retriever:
                      type: SimpleRetriever
                      requester:
                        type: HttpRequester
                        url_base: %s
                        path: "/things"
                        authenticator:
                          type: BearerAuthenticator
                          api_token: "{{ config['token'] }}"
                      record_selector:
                        type: RecordSelector
                        extractor:
                          type: DpathExtractor
                          field_path: ["content"]
                spec:
                  type: Spec
                  connection_specification:
                    type: object
                    properties: {}
                """
                        .formatted(server.url("/").toString().replaceAll("/$", ""));
        ManifestPipeline.Manifest parsed = new ManifestPipeline().process(yaml);
        StreamSpec stream = new StreamSpec(parsed.stream("things"));
        ManifestPollTask task =
                new ManifestPollTask(
                        null, CategoryType.HISTORICAL, stream, client, Map.of("token", "t0k3n"));
        assertEquals(0, task.readStream(r -> {}));
        RecordedRequest request = server.takeRequest();
        assertEquals("Bearer t0k3n", request.getHeader("Authorization"));
    }
}
