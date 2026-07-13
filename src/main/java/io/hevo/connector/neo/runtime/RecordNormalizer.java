package io.hevo.connector.neo.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coerces raw JSON record values into the exact Java types the publish-time converter casts to
 * ({@code HDatumUtils}: JSON→String, DOUBLE→Double, LONG→Long, VARCHAR→String, …).
 *
 * <p>A hand-written connector does this implicitly by mapping responses onto typed POJOs; neo
 * publishes parsed JSON as-is, so without this step a JSON-schema {@code number} field whose API
 * value happens to be an integer (or an {@code object} field arriving as a parsed Map) fails the
 * strict cast with "Data conversion failed".
 */
public final class RecordNormalizer {

  private static final Logger log = LoggerFactory.getLogger(RecordNormalizer.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final Map<String, String> hevoTypeByField;

  public RecordNormalizer(StreamSpec stream) {
    this.hevoTypeByField = SchemaSynthesizer.fieldHevoTypes(stream);
  }

  /** Returns a copy of the record with every schema-declared field coerced to its Hevo type. */
  public Map<String, Object> normalize(Map<String, Object> record) {
    Map<String, Object> normalized = new LinkedHashMap<>(record);
    for (Map.Entry<String, String> field : hevoTypeByField.entrySet()) {
      Object value = normalized.get(field.getKey());
      if (value != null) {
        normalized.put(field.getKey(), coerce(field.getValue(), value));
      }
    }
    return normalized;
  }

  private Object coerce(String hevoType, Object value) {
    return switch (hevoType) {
      case "JSON" -> value instanceof String ? value : toJsonString(value);
      case "DOUBLE" -> value instanceof Double
          ? value
          : value instanceof Number number ? number.doubleValue() : parseDouble(value);
      case "LONG" -> value instanceof Long
          ? value
          : value instanceof Number number ? number.longValue() : parseLong(value);
      case "BOOLEAN" -> value instanceof Boolean
          ? value
          : value instanceof String s ? Boolean.parseBoolean(s) : value;
      case "VARCHAR" -> value instanceof String
          ? value
          : value instanceof Map || value instanceof List
              ? toJsonString(value)
              : String.valueOf(value);
      default -> value;
    };
  }

  private static Object parseDouble(Object value) {
    try {
      return Double.valueOf(String.valueOf(value));
    } catch (NumberFormatException e) {
      log.warn("Could not coerce value to DOUBLE, passing through: {}", value);
      return value;
    }
  }

  private static Object parseLong(Object value) {
    try {
      return Long.valueOf(String.valueOf(value));
    } catch (NumberFormatException e) {
      log.warn("Could not coerce value to LONG, passing through: {}", value);
      return value;
    }
  }

  private static String toJsonString(Object value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (Exception e) {
      throw new ManifestException("Failed to serialize field value to JSON", e);
    }
  }
}
