package io.hevo.connector.neo.manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** End-to-end pipeline test against the real source-jotform manifest from the airbyte repo. */
class ManifestPipelineTest {

    private final ManifestPipeline pipeline = new ManifestPipeline();

    private ManifestPipeline.Manifest jotform() {
        InputStream yaml = getClass().getResourceAsStream("/manifests/jotform.yaml");
        assertNotNull(yaml, "test manifest missing");
        return pipeline.process(yaml);
    }

    @Test
    @SuppressWarnings("unchecked")
    void resolvesAndValidatesJotform() {
        ManifestPipeline.Manifest manifest = jotform();
        assertEquals(6, manifest.streams().size());

        // The streams list entries were $ref pointers into definitions; they must be materialized.
        Map<String, Object> forms = manifest.stream("forms");
        assertNotNull(forms);
        assertEquals("DeclarativeStream", forms.get("type"));

        Map<String, Object> retriever = (Map<String, Object>) forms.get("retriever");
        assertEquals("SimpleRetriever", retriever.get("type"));

        // base_requester is shared via $ref; the url_base must be merged into each requester.
        Map<String, Object> requester = (Map<String, Object>) retriever.get("requester");
        assertTrue(requester.containsKey("url_base"));
        Map<String, Object> authenticator = (Map<String, Object>) requester.get("authenticator");
        assertEquals("ApiKeyAuthenticator", authenticator.get("type"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void substreamKeepsParentStreamConfig() {
        ManifestPipeline.Manifest manifest = jotform();
        Map<String, Object> questions = manifest.stream("questions");
        assertNotNull(questions);
        Map<String, Object> retriever = (Map<String, Object>) questions.get("retriever");
        Map<String, Object> router = (Map<String, Object>) retriever.get("partition_router");
        assertEquals("SubstreamPartitionRouter", router.get("type"));
        List<Map<String, Object>> parents =
                (List<Map<String, Object>>) router.get("parent_stream_configs");
        assertEquals("ParentStreamConfig", parents.get(0).get("type"));
    }

    @Test
    void specSurvivesPipeline() {
        ManifestPipeline.Manifest manifest = jotform();
        assertNotNull(manifest.spec());
        assertEquals("Spec", manifest.spec().get("type"));
        assertNotNull(manifest.version());
    }

    @Test
    void invalidManifestFailsValidation() {
        String invalid =
                """
                version: 4.6.2
                type: DeclarativeSource
                check:
                  type: CheckStream
                  stream_names: ["x"]
                streams:
                  - type: DeclarativeStream
                    retriever:
                      type: SimpleRetriever
                      requester:
                        type: HttpRequester
                        http_method: NOT_A_METHOD
                """;
        assertThrows(ManifestException.class, () -> pipeline.process(invalid));
    }
}
