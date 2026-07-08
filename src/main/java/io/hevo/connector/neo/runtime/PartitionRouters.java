package io.hevo.connector.neo.runtime;

import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition routers, mirroring partition_routers/{list_partition_router.py,
 * substream_partition_router.py}. A partition is the {@code stream_partition} template context
 * plus any request options the router injects.
 *
 * <p>P2 scope: ListPartitionRouter and SubstreamPartitionRouter (parents read inline as full
 * refresh, matching Airbyte's semantics of iterating parent records; incremental_dependency and
 * extra_fields are not supported yet).
 */
public final class PartitionRouters {

    private static final Logger log = LoggerFactory.getLogger(PartitionRouters.class);

    /** One unit of work: the partition values and per-target request options. */
    public record Partition(
            Map<String, Object> values,
            Map<RequestOptionSpec.InjectInto, Map<String, Object>> requestOptions) {

        public static Partition empty() {
            return new Partition(Map.of(), Map.of());
        }
    }

    public interface Router {
        List<Partition> partitions();
    }

    private PartitionRouters() {}

    /**
     * @param parentReader reads all records of a parent DeclarativeStream (resolved map), full
     *     refresh — used by SubstreamPartitionRouter.
     */
    public static Router from(
            Map<String, Object> component,
            Map<String, Object> config,
            Function<Map<String, Object>, List<Object>> parentReader) {
        if (component == null) {
            return () -> List.of(Partition.empty());
        }
        String type = Components.type(component);
        return switch (type == null ? "" : type) {
            case "ListPartitionRouter" -> new ListRouter(component, config);
            case "SubstreamPartitionRouter" -> new SubstreamRouter(component, config, parentReader);
            default -> throw new ManifestException("Unsupported partition_router type: " + type);
        };
    }

    static final class ListRouter implements Router {
        private final Map<String, Object> config;
        private final String cursorField;
        private final Object values;
        private final Map<String, Object> parameters;
        private final RequestOptionSpec requestOption;

        ListRouter(Map<String, Object> component, Map<String, Object> config) {
            this.config = config;
            this.parameters = Components.parameters(component);
            String field = Components.getString(component, "cursor_field");
            if (field == null) {
                throw new ManifestException("ListPartitionRouter requires cursor_field");
            }
            this.cursorField =
                    String.valueOf(InterpolatedString.create(field, parameters).eval(config));
            this.values = component.get("values");
            this.requestOption =
                    RequestOptionSpec.from(Components.getMap(component, "request_option"));
        }

        @Override
        public List<Partition> partitions() {
            List<?> sliceValues;
            if (values instanceof List<?> list) {
                sliceValues = list;
            } else if (values instanceof String s) {
                Object evaluated = InterpolatedString.create(s, parameters).eval(config);
                if (!(evaluated instanceof List<?> list)) {
                    throw new ManifestException(
                            "ListPartitionRouter values did not evaluate to a list: " + evaluated);
                }
                sliceValues = list;
            } else {
                throw new ManifestException("ListPartitionRouter requires values");
            }
            List<Partition> partitions = new ArrayList<>();
            for (Object value : sliceValues) {
                partitions.add(
                        new Partition(
                                Map.of(cursorField, value),
                                optionsFor(requestOption, config, value)));
            }
            return partitions;
        }
    }

    static final class SubstreamRouter implements Router {
        private final Map<String, Object> config;
        private final List<Map<String, Object>> parentConfigs = new ArrayList<>();
        private final Function<Map<String, Object>, List<Object>> parentReader;

        SubstreamRouter(
                Map<String, Object> component,
                Map<String, Object> config,
                Function<Map<String, Object>, List<Object>> parentReader) {
            this.config = config;
            this.parentReader = parentReader;
            List<Object> configs = Components.getList(component, "parent_stream_configs");
            if (configs == null || configs.isEmpty()) {
                throw new ManifestException(
                        "SubstreamPartitionRouter requires parent_stream_configs");
            }
            for (Object parent : configs) {
                if (parent instanceof Map<?, ?>) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> parentConfig = (Map<String, Object>) parent;
                    parentConfigs.add(parentConfig);
                    if (Boolean.TRUE.equals(parentConfig.get("incremental_dependency"))) {
                        log.warn(
                                "SubstreamPartitionRouter: incremental_dependency is not"
                                        + " supported yet; parent state is not tracked");
                    }
                }
            }
        }

        @Override
        public List<Partition> partitions() {
            List<Partition> partitions = new ArrayList<>();
            for (Map<String, Object> parentConfig : parentConfigs) {
                Map<String, Object> parameters = Components.parameters(parentConfig);
                String parentKey = Components.getString(parentConfig, "parent_key");
                String partitionField = Components.getString(parentConfig, "partition_field");
                if (parentKey == null || partitionField == null) {
                    throw new ManifestException(
                            "ParentStreamConfig requires parent_key and partition_field");
                }
                String evaluatedKey =
                        String.valueOf(
                                InterpolatedString.create(parentKey, parameters).eval(config));
                String evaluatedField =
                        String.valueOf(
                                InterpolatedString.create(partitionField, parameters)
                                        .eval(config));
                RequestOptionSpec option =
                        RequestOptionSpec.from(Components.getMap(parentConfig, "request_option"));
                Map<String, Object> parentStream = Components.getMap(parentConfig, "stream");
                if (parentStream == null) {
                    throw new ManifestException("ParentStreamConfig requires stream");
                }
                for (Object parentRecord : parentReader.apply(parentStream)) {
                    if (!(parentRecord instanceof Map<?, ?> record)) {
                        continue;
                    }
                    Object value = dpathGet(record, evaluatedKey);
                    if (value == null) {
                        // Python skips slices whose parent record lacks the key.
                        continue;
                    }
                    Map<String, Object> partitionValues = new LinkedHashMap<>();
                    partitionValues.put(evaluatedField, value);
                    partitionValues.put("parent_slice", Map.of());
                    partitions.add(
                            new Partition(
                                    partitionValues, optionsFor(option, config, value)));
                }
            }
            return partitions;
        }

        /** parent_key may be a plain key or a "/"-separated path into the parent record. */
        private static Object dpathGet(Map<?, ?> record, String key) {
            if (record.containsKey(key)) {
                return record.get(key);
            }
            Object node = record;
            for (String segment : key.split("/")) {
                if (!(node instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                    return null;
                }
                node = map.get(segment);
            }
            return node;
        }
    }

    private static Map<RequestOptionSpec.InjectInto, Map<String, Object>> optionsFor(
            RequestOptionSpec option, Map<String, Object> config, Object value) {
        if (option == null || option.fieldName() == null || value == null) {
            return Map.of();
        }
        // Python only injects truthy values.
        if ("".equals(value) || Boolean.FALSE.equals(value)) {
            return Map.of();
        }
        return Map.of(
                option.injectInto(),
                Map.of(String.valueOf(option.fieldName().eval(config)), value));
    }
}
