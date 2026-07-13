package io.hevo.connector.neo.runtime;

import java.util.List;
import java.util.Map;

/** Helpers for reading resolved manifest component maps. */
public final class Components {

  private Components() {}

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getMap(Map<String, Object> component, String key) {
    Object value = component == null ? null : component.get(key);
    return value instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
  }

  @SuppressWarnings("unchecked")
  public static List<Object> getList(Map<String, Object> component, String key) {
    Object value = component == null ? null : component.get(key);
    return value instanceof List<?> list ? (List<Object>) list : null;
  }

  public static String getString(Map<String, Object> component, String key) {
    Object value = component == null ? null : component.get(key);
    return value == null ? null : String.valueOf(value);
  }

  public static String type(Map<String, Object> component) {
    return getString(component, "type");
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> parameters(Map<String, Object> component) {
    Map<String, Object> parameters = getMap(component, "$parameters");
    return parameters == null ? Map.of() : parameters;
  }
}
