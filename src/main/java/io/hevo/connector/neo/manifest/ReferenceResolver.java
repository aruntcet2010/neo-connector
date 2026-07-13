package io.hevo.connector.neo.manifest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves "$ref" keys and bare "#/path" string references in a manifest, producing a fully
 * dereferenced structure.
 *
 * <p>Port of airbyte_cdk/sources/declarative/parsers/manifest_reference_resolver.py. Semantics:
 *
 * <ul>
 *   <li>A dict containing {@code $ref: "#/definitions/x"} is replaced by the referenced value
 *       merged with the dict's own sibling keys; sibling keys take precedence.
 *   <li>A bare string value {@code "#/definitions/x"} is replaced by the referenced value.
 *   <li>Reference paths are ambiguous ("foo/bar" may be a nested path or a literal key containing a
 *       slash): the full remaining path is tried as a literal key at each level before descending
 *       one segment.
 *   <li>Numeric path segments index into lists.
 *   <li>Circular references raise {@link ManifestException.CircularReference}.
 * </ul>
 */
public final class ReferenceResolver {

  private static final String REF_TAG = "$ref";

  public Map<String, Object> preprocessManifest(Map<String, Object> manifest) {
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) evaluateNode(manifest, manifest, new HashSet<>());
    return result;
  }

  private Object evaluateNode(Object node, Map<String, Object> manifest, Set<String> visited) {
    if (node instanceof Map<?, ?> map) {
      Map<String, Object> evaluated = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String key = String.valueOf(entry.getKey());
        if (!REF_TAG.equals(key)) {
          evaluated.put(key, evaluateNode(entry.getValue(), manifest, visited));
        }
      }
      if (map.containsKey(REF_TAG)) {
        Object evaluatedRef = evaluateNode(map.get(REF_TAG), manifest, visited);
        if (!(evaluatedRef instanceof Map<?, ?> refMap)) {
          return evaluatedRef;
        }
        // Values defined on the component take precedence over the reference values.
        Map<String, Object> merged = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : refMap.entrySet()) {
          merged.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        merged.putAll(evaluated);
        return merged;
      }
      return evaluated;
    }
    if (node instanceof List<?> list) {
      List<Object> evaluated = new ArrayList<>(list.size());
      for (Object item : list) {
        evaluated.add(evaluateNode(item, manifest, visited));
      }
      return evaluated;
    }
    if (isRef(node)) {
      String ref = (String) node;
      if (visited.contains(ref)) {
        throw new ManifestException.CircularReference(ref);
      }
      visited.add(ref);
      Object result = evaluateNode(lookupRefValue(ref, manifest), manifest, visited);
      visited.remove(ref);
      return result;
    }
    return node;
  }

  private static boolean isRef(Object node) {
    return node instanceof String s && s.startsWith("#/");
  }

  private Object lookupRefValue(String ref, Map<String, Object> manifest) {
    String path = ref.substring(2);
    try {
      return readRefValue(path, manifest);
    } catch (ManifestException.UndefinedReference e) {
      throw e;
    } catch (RuntimeException e) {
      throw new ManifestException.UndefinedReference(path, ref);
    }
  }

  /**
   * Walks {@code ref} into {@code node}: tries the full remaining path as a literal key first, then
   * peels one segment (string key or int list index) and descends.
   */
  private Object readRefValue(String ref, Object node) {
    while (!ref.isEmpty()) {
      Object direct = tryGet(node, ref);
      if (direct != SENTINEL) {
        return direct;
      }
      int slash = ref.indexOf('/');
      String head = slash < 0 ? ref : ref.substring(0, slash);
      String rest = slash < 0 ? "" : ref.substring(slash + 1);
      Object next = tryGet(node, head);
      if (next == SENTINEL) {
        throw new ManifestException.UndefinedReference(ref, "#/" + ref);
      }
      node = next;
      ref = rest;
    }
    return node;
  }

  private static final Object SENTINEL = new Object();

  /** Fetches {@code key} from a map (string key) or list (integer index); SENTINEL if absent. */
  private static Object tryGet(Object node, String key) {
    if (node instanceof Map<?, ?> map) {
      return map.containsKey(key) ? map.get(key) : SENTINEL;
    }
    if (node instanceof List<?> list) {
      try {
        int index = Integer.parseInt(key);
        return index >= 0 && index < list.size() ? list.get(index) : SENTINEL;
      } catch (NumberFormatException e) {
        return SENTINEL;
      }
    }
    return SENTINEL;
  }
}
