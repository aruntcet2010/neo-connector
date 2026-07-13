package io.hevo.connector.neo.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Record extraction by field path, mirroring the CDK's DpathExtractor semantics
 * (extractors/dpath_extractor.py):
 *
 * <ul>
 *   <li>Empty field_path → the whole decoded body is the record set.
 *   <li>Path segments are interpolated; {@code *} wildcards fan out across map values / list
 *       elements.
 *   <li>A list result yields each element; a non-empty map yields itself as a single record; a
 *       missing path yields nothing.
 * </ul>
 */
public final class DpathExtractor {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final List<InterpolatedString> fieldPath;
  private final Map<String, Object> config;

  public DpathExtractor(Map<String, Object> component, Map<String, Object> config) {
    this.config = config;
    List<Object> path = Components.getList(component, "field_path");
    this.fieldPath = new ArrayList<>();
    if (path != null) {
      Map<String, Object> parameters = Components.parameters(component);
      for (Object segment : path) {
        fieldPath.add(InterpolatedString.create(String.valueOf(segment), parameters));
      }
    }
  }

  public List<Object> extractRecords(String responseBody) {
    Object body = decode(responseBody);
    if (fieldPath.isEmpty()) {
      return wrap(body);
    }
    List<String> path = new ArrayList<>(fieldPath.size());
    boolean hasWildcard = false;
    for (InterpolatedString segment : fieldPath) {
      String evaluated = String.valueOf(segment.eval(config));
      hasWildcard |= "*".equals(evaluated);
      path.add(evaluated);
    }
    List<Object> matches = new ArrayList<>();
    walk(body, path, 0, matches);
    if (hasWildcard) {
      return matches;
    }
    // Without wildcards there is at most one match; unwrap it like dpath.get(default=[]).
    Object extracted = matches.isEmpty() ? List.of() : matches.get(0);
    return wrap(extracted);
  }

  private static List<Object> wrap(Object extracted) {
    if (extracted instanceof List<?> list) {
      return new ArrayList<>(list);
    }
    List<Object> records = new ArrayList<>();
    if (extracted instanceof Map<?, ?> map) {
      if (!map.isEmpty()) {
        records.add(extracted);
      }
    } else if (extracted != null && !"".equals(extracted)) {
      records.add(extracted);
    }
    return records;
  }

  private static void walk(Object node, List<String> path, int index, List<Object> matches) {
    if (index == path.size()) {
      matches.add(node);
      return;
    }
    String segment = path.get(index);
    if ("*".equals(segment)) {
      if (node instanceof Map<?, ?> map) {
        for (Object value : map.values()) {
          walk(value, path, index + 1, matches);
        }
      } else if (node instanceof List<?> list) {
        for (Object value : list) {
          walk(value, path, index + 1, matches);
        }
      }
      return;
    }
    if (node instanceof Map<?, ?> map && map.containsKey(segment)) {
      walk(map.get(segment), path, index + 1, matches);
    } else if (node instanceof List<?> list) {
      try {
        int i = Integer.parseInt(segment);
        if (i >= 0 && i < list.size()) {
          walk(list.get(i), path, index + 1, matches);
        }
      } catch (NumberFormatException ignored) {
        // not a list index; no match
      }
    }
  }

  static Object decode(String responseBody) {
    if (responseBody == null || responseBody.isEmpty()) {
      return Map.of();
    }
    try {
      return JSON.readValue(responseBody, Object.class);
    } catch (Exception e) {
      throw new ManifestException("Response body is not valid JSON", e);
    }
  }
}
