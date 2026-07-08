package io.hevo.connector.neo.manifest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/** Parses a manifest.yaml document into plain Java maps and lists. */
public final class ManifestLoader {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private ManifestLoader() {}

    public static Map<String, Object> load(String yaml) {
        try {
            return YAML.readValue(yaml, MAP_TYPE);
        } catch (IOException e) {
            throw new ManifestException("Failed to parse manifest YAML", e);
        }
    }

    public static Map<String, Object> load(InputStream yaml) {
        try {
            return YAML.readValue(yaml, MAP_TYPE);
        } catch (IOException e) {
            throw new ManifestException("Failed to parse manifest YAML", e);
        }
    }
}
