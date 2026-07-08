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
 * P1 milestone: the REAL source-jotform manifest (unmodified, from the airbyte repo) syncs its
 * `forms` stream end-to-end against a mock Jotform API. The mock is reached by setting
 * config.api_endpoint.enterprise_url, which the manifest's url_base template prefers when set.
 */
class JotformEndToEndTest {

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
                "api_key", "jotform-key",
                "start_date", "2024-01-01T00:00:00Z",
                "api_endpoint",
                        Map.of("enterprise_url", server.url("/").toString().replaceAll("/$", "")));
    }

    private StreamSpec forms() {
        ManifestPipeline.Manifest manifest =
                new ManifestPipeline()
                        .process(getClass().getResourceAsStream("/manifests/jotform.yaml"));
        return new StreamSpec(manifest.stream("forms"));
    }

    @Test
    void formsStreamSyncsEndToEnd() throws Exception {
        // Jotform pages by offset; page size in the manifest's paginator.
        server.enqueue(
                new MockResponse()
                        .setBody(
                                """
                                {"responseCode": 200, "content": [
                                  {"id": "240101", "title": "Form A", "status": "ENABLED",
                                   "created_at": "2024-01-05 10:00:00", "updated_at": "2024-02-01 10:00:00"},
                                  {"id": "240102", "title": "Form B", "status": "ENABLED",
                                   "created_at": "2024-01-06 10:00:00", "updated_at": "2024-02-02 10:00:00"}
                                ]}
                                """));
        // Short page ends pagination (fewer records than the manifest's page size).
        server.enqueue(new MockResponse().setBody("{\"responseCode\": 200, \"content\": []}"));

        ManifestPollTask task =
                new ManifestPollTask(null, CategoryType.HISTORICAL, forms(), client, config());
        List<Object> records = new ArrayList<>();
        long count = task.readStream(records::add);

        assertEquals(2, count);
        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) records.get(0);
        assertEquals("240101", first.get("id"));
        assertEquals("Form A", first.get("title"));

        RecordedRequest request = server.takeRequest();
        assertNotNull(request.getPath());
        assertTrue(request.getPath().startsWith("/user/forms"), request.getPath());
        assertTrue(request.getPath().contains("apiKey=jotform-key"), request.getPath());
    }

    @Test
    void formsSchemaSynthesizes() {
        ObjectSchema schema = SchemaSynthesizer.synthesize(forms());
        assertEquals("forms", schema.objectDetail().getTableFullyQualifiedName());
        assertTrue(schema.fields().size() > 5, "expected a real field set");
        var id =
                schema.fields().stream()
                        .filter(f -> f.name().equals("id"))
                        .findFirst()
                        .orElseThrow();
        assertEquals("hudt_varchar", id.logicalType());
    }
}
