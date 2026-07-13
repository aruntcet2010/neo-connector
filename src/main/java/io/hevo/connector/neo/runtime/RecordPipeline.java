package io.hevo.connector.neo.runtime;

import io.hevo.connector.neo.interpolation.InterpolatedBoolean;
import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Post-extraction record processing, mirroring the CDK's RecordSelector.filter_and_transform
 * default order: filter first, then transformations (AddFields / RemoveFields) in list order.
 */
public final class RecordPipeline {

  private final Map<String, Object> config;
  private final InterpolatedBoolean filterCondition;
  private final List<Transformation> transformations = new ArrayList<>();

  public RecordPipeline(
      Map<String, Object> recordSelector,
      List<Object> transformationComponents,
      Map<String, Object> config) {
    this.config = config;
    Map<String, Object> recordFilter = Components.getMap(recordSelector, "record_filter");
    String condition =
        recordFilter == null ? null : Components.getString(recordFilter, "condition");
    this.filterCondition =
        condition == null
            ? null
            : InterpolatedBoolean.create(condition, Components.parameters(recordFilter));
    if (transformationComponents != null) {
      for (Object component : transformationComponents) {
        if (component instanceof Map<?, ?>) {
          @SuppressWarnings("unchecked")
          Map<String, Object> map = (Map<String, Object>) component;
          transformations.add(Transformation.from(map));
        }
      }
    }
  }

  /** Returns the processed record, or null when the filter drops it. */
  public Map<String, Object> process(Map<String, Object> record, Map<String, Object> streamSlice) {
    Map<String, Object> context = new LinkedHashMap<>();
    context.put("record", record);
    context.put("stream_slice", streamSlice);
    if (filterCondition != null && !filterCondition.eval(config, context)) {
      return null;
    }
    Map<String, Object> current = record;
    for (Transformation transformation : transformations) {
      current = transformation.apply(current, streamSlice, config);
    }
    return current;
  }

  private interface Transformation {
    Map<String, Object> apply(
        Map<String, Object> record, Map<String, Object> streamSlice, Map<String, Object> config);

    static Transformation from(Map<String, Object> component) {
      String type = Components.type(component);
      return switch (type == null ? "" : type) {
        case "AddFields" -> new AddFields(component);
        case "RemoveFields" -> new RemoveFields(component);
        default -> throw new ManifestException("Unsupported transformation type: " + type);
      };
    }
  }

  private static final class AddFields implements Transformation {
    private record FieldSpec(List<String> path, InterpolatedString value) {}

    private final List<FieldSpec> fields = new ArrayList<>();

    AddFields(Map<String, Object> component) {
      Map<String, Object> parameters = Components.parameters(component);
      List<Object> fieldComponents = Components.getList(component, "fields");
      if (fieldComponents == null) {
        throw new ManifestException("AddFields requires fields");
      }
      for (Object fieldComponent : fieldComponents) {
        if (!(fieldComponent instanceof Map<?, ?>)) {
          continue;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) fieldComponent;
        List<Object> path = Components.getList(map, "path");
        String value = Components.getString(map, "value");
        if (path == null || path.isEmpty() || value == null) {
          throw new ManifestException("AddedFieldDefinition requires path and value");
        }
        List<String> segments = path.stream().map(String::valueOf).toList();
        fields.add(new FieldSpec(segments, InterpolatedString.create(value, parameters)));
      }
    }

    @Override
    public Map<String, Object> apply(
        Map<String, Object> record, Map<String, Object> streamSlice, Map<String, Object> config) {
      Map<String, Object> mutable = new LinkedHashMap<>(record);
      for (FieldSpec field : fields) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("record", record);
        context.put("stream_slice", streamSlice);
        Object value = field.value().eval(config, context);
        setPath(mutable, field.path(), value);
      }
      return mutable;
    }

    @SuppressWarnings("unchecked")
    private static void setPath(Map<String, Object> record, List<String> path, Object value) {
      Map<String, Object> node = record;
      for (int i = 0; i < path.size() - 1; i++) {
        Object next = node.get(path.get(i));
        if (!(next instanceof Map<?, ?>)) {
          next = new LinkedHashMap<String, Object>();
          node.put(path.get(i), next);
        }
        node = (Map<String, Object>) next;
      }
      node.put(path.get(path.size() - 1), value);
    }
  }

  private static final class RemoveFields implements Transformation {
    private final List<List<String>> pointers = new ArrayList<>();

    RemoveFields(Map<String, Object> component) {
      List<Object> fieldPointers = Components.getList(component, "field_pointers");
      if (fieldPointers == null) {
        throw new ManifestException("RemoveFields requires field_pointers");
      }
      for (Object pointer : fieldPointers) {
        if (pointer instanceof List<?> list) {
          pointers.add(list.stream().map(String::valueOf).toList());
        } else {
          pointers.add(List.of(String.valueOf(pointer)));
        }
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> apply(
        Map<String, Object> record, Map<String, Object> streamSlice, Map<String, Object> config) {
      Map<String, Object> mutable = new LinkedHashMap<>(record);
      for (List<String> pointer : pointers) {
        Map<String, Object> node = mutable;
        boolean reachable = true;
        for (int i = 0; i < pointer.size() - 1 && reachable; i++) {
          Object next = node.get(pointer.get(i));
          if (next instanceof Map<?, ?> map) {
            node = (Map<String, Object>) map;
          } else {
            reachable = false;
          }
        }
        if (reachable) {
          node.remove(pointer.get(pointer.size() - 1));
        }
      }
      return mutable;
    }
  }
}
