package io.hevo.connector.neo.manifest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates a fully resolved manifest against the Airbyte declarative component schema
 * (declarative_component_schema.yaml, copied verbatim from airbyte-python-cdk).
 */
public final class ManifestValidator {

    private static final String SCHEMA_RESOURCE = "/manifest/declarative_component_schema.yaml";

    private final JsonSchema schema;
    private final ObjectMapper mapper = new ObjectMapper();

    public ManifestValidator() {
        try (InputStream in = ManifestValidator.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new ManifestException(
                        "Missing bundled component schema: " + SCHEMA_RESOURCE);
            }
            JsonNode schemaNode = new ObjectMapper(new YAMLFactory()).readTree(in);
            JsonSchemaFactory factory =
                    JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            SchemaValidatorsConfig config = SchemaValidatorsConfig.builder().build();
            this.schema = factory.getSchema(schemaNode, config);
        } catch (IOException e) {
            throw new ManifestException("Failed to read bundled component schema", e);
        }
    }

    /** Throws {@link ManifestException} listing every violation if the manifest is invalid. */
    public void validate(Map<String, Object> resolvedManifest) {
        JsonNode manifestNode = mapper.valueToTree(resolvedManifest);
        Set<ValidationMessage> errors = schema.validate(manifestNode);
        if (!errors.isEmpty()) {
            String details =
                    errors.stream()
                            .map(ValidationMessage::getMessage)
                            .sorted()
                            .collect(Collectors.joining("\n  "));
            throw new ManifestException(
                    "Manifest failed validation against the declarative component schema:\n  "
                            + details);
        }
    }
}
