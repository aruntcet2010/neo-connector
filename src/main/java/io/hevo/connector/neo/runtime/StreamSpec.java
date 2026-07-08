package io.hevo.connector.neo.runtime;

import io.hevo.catalog.core.namespace.Namespace;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One stream's resolved manifest configuration, with the pieces the runtime needs. */
public final class StreamSpec {

    private static final Logger log = LoggerFactory.getLogger(StreamSpec.class);

    private final String name;
    private final List<String> primaryKey;
    private final Map<String, Object> retriever;
    private final Map<String, Object> schema;
    private final Map<String, Object> incrementalSync;
    private final List<Object> transformations;

    @SuppressWarnings("unchecked")
    public StreamSpec(Map<String, Object> stream) {
        this.name = Components.getString(stream, "name");
        if (name == null) {
            throw new ManifestException("DeclarativeStream missing name: " + stream);
        }
        this.retriever = Components.getMap(stream, "retriever");
        if (retriever == null) {
            throw new ManifestException("Stream " + name + " has no retriever");
        }
        String retrieverType = Components.type(retriever);
        if (!"SimpleRetriever".equals(retrieverType)) {
            throw new ManifestException(
                    "Stream " + name + ": unsupported retriever type " + retrieverType);
        }
        this.incrementalSync = Components.getMap(stream, "incremental_sync");
        this.transformations = Components.getList(stream, "transformations");
        this.primaryKey = parsePrimaryKey(stream.get("primary_key"));
        Map<String, Object> schemaLoader = Components.getMap(stream, "schema_loader");
        this.schema =
                schemaLoader != null && "InlineSchemaLoader".equals(Components.type(schemaLoader))
                        ? Components.getMap(schemaLoader, "schema")
                        : null;
    }

    private static List<String> parsePrimaryKey(Object primaryKey) {
        List<String> keys = new ArrayList<>();
        if (primaryKey instanceof String s) {
            keys.add(s);
        } else if (primaryKey instanceof List<?> list) {
            for (Object key : list) {
                if (key instanceof List<?> nested) {
                    // composite nested keys: use the last segment
                    if (!nested.isEmpty()) {
                        keys.add(String.valueOf(nested.get(nested.size() - 1)));
                    }
                } else {
                    keys.add(String.valueOf(key));
                }
            }
        }
        return keys;
    }

    public String name() {
        return name;
    }

    public Namespace namespace() {
        return new Namespace(name, null, null);
    }

    public List<String> primaryKey() {
        return primaryKey;
    }

    public Map<String, Object> requester() {
        return Components.getMap(retriever, "requester");
    }

    public Map<String, Object> paginator() {
        return Components.getMap(retriever, "paginator");
    }

    public Map<String, Object> recordSelector() {
        return Components.getMap(retriever, "record_selector");
    }

    public Map<String, Object> partitionRouter() {
        return Components.getMap(retriever, "partition_router");
    }

    public Map<String, Object> incrementalSync() {
        return incrementalSync;
    }

    public List<Object> transformations() {
        return transformations;
    }

    public Map<String, Object> extractor() {
        return Components.getMap(recordSelector(), "extractor");
    }

    /** The stream's JSON schema from InlineSchemaLoader, or null. */
    public Map<String, Object> jsonSchema() {
        return schema;
    }
}
