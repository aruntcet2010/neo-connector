package io.hevo.connector.neo.manifest;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Turns raw manifest YAML into a fully resolved, validated component tree, mirroring the Python
 * CDK's pre-processing order: parse → resolve {@code $ref}s → propagate types/{@code $parameters}
 * → schema-validate.
 */
public final class ManifestPipeline {

    private final ReferenceResolver referenceResolver = new ReferenceResolver();
    private final ComponentTransformer componentTransformer = new ComponentTransformer();
    private final ManifestValidator validator = new ManifestValidator();

    public Manifest process(String manifestYaml) {
        return process(ManifestLoader.load(manifestYaml));
    }

    public Manifest process(InputStream manifestYaml) {
        return process(ManifestLoader.load(manifestYaml));
    }

    public Manifest process(Map<String, Object> rawManifest) {
        Map<String, Object> resolved = referenceResolver.preprocessManifest(rawManifest);
        Map<String, Object> propagated =
                componentTransformer.propagateTypesAndParameters("", resolved, Map.of());
        validator.validate(propagated);
        return new Manifest(propagated);
    }

    /** A resolved and validated manifest with typed accessors for its top-level sections. */
    public record Manifest(Map<String, Object> resolved) {

        public String version() {
            return (String) resolved.get("version");
        }

        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> streams() {
            List<Map<String, Object>> streams =
                    (List<Map<String, Object>>) resolved.get("streams");
            return streams == null ? List.of() : streams;
        }

        @SuppressWarnings("unchecked")
        public Map<String, Object> spec() {
            return (Map<String, Object>) resolved.get("spec");
        }

        @SuppressWarnings("unchecked")
        public Map<String, Object> check() {
            return (Map<String, Object>) resolved.get("check");
        }

        /** The stream with the given `name`, or null. */
        public Map<String, Object> stream(String name) {
            for (Map<String, Object> stream : streams()) {
                if (name.equals(stream.get("name"))) {
                    return stream;
                }
            }
            return null;
        }
    }
}
