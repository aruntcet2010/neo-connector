package io.hevo.connector.neo.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.neo.manifest.ManifestException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
      case "BOOLEAN" -> coerceBoolean(value);
      case "DATE_TIME_TZ" -> coerceDateTimeTz(value);
      case "DATE" -> coerceDate(value);
      case "VARCHAR" -> value instanceof String
          ? value
          : value instanceof Map || value instanceof List
              ? toJsonString(value)
              : String.valueOf(value);
      default -> value;
    };
  }

  /**
   * The publisher casts BOOLEAN fields with {@code (Boolean) colVal}, so numeric 0/1 and string
   * truth-words must become real Booleans. String semantics follow Python's strtobool (the same
   * rule Airbyte's TypeTransformer uses): y/yes/t/true/on/1 and n/no/f/false/off/0; anything else
   * passes through with a warning.
   */
  private static Object coerceBoolean(Object value) {
    if (value instanceof Boolean) {
      return value;
    }
    if (value instanceof Number number) {
      return number.doubleValue() != 0.0;
    }
    if (value instanceof String s) {
      switch (s.toLowerCase().strip()) {
        case "y", "yes", "t", "true", "on", "1":
          return Boolean.TRUE;
        case "n", "no", "f", "false", "off", "0":
          return Boolean.FALSE;
        default:
          // fall through to warn
      }
    }
    log.warn("Could not coerce value to BOOLEAN, passing through: {}", value);
    return value;
  }

  /**
   * The publisher parses DATE_TIME_TZ fields with {@code OffsetDateTime.parse((String) colVal)} —
   * strict ISO-8601 with a mandatory offset. Real APIs send naive timestamps ("2024-01-05
   * 10:00:00"), ISO without offset, dates, or epoch numbers; canonicalize them all.
   */
  private static Object coerceDateTimeTz(Object value) {
    OffsetDateTime parsed = parseFlexibleDateTime(value);
    if (parsed == null) {
      log.warn("Could not coerce value to DATE_TIME_TZ, passing through: {}", value);
      return value;
    }
    return parsed.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  }

  /**
   * The publisher casts DATE fields with {@code ((java.sql.Date) colVal).toLocalDate()} — it wants
   * an actual java.sql.Date object, never a string.
   */
  private static Object coerceDate(Object value) {
    if (value instanceof java.sql.Date) {
      return value;
    }
    if (value instanceof String s) {
      try {
        return java.sql.Date.valueOf(LocalDate.parse(s.strip().substring(0, Math.min(10, s.strip().length()))));
      } catch (RuntimeException ignored) {
        // fall through to full datetime parsing
      }
    }
    OffsetDateTime parsed = parseFlexibleDateTime(value);
    if (parsed == null) {
      log.warn("Could not coerce value to DATE, passing through: {}", value);
      return value;
    }
    return java.sql.Date.valueOf(parsed.toLocalDate());
  }

  /** ISO w/ offset, ISO naive (assumed UTC), space-separated naive, date-only, or epoch number. */
  private static OffsetDateTime parseFlexibleDateTime(Object value) {
    if (value instanceof Number number) {
      long epoch = number.longValue();
      // Heuristic: values past ~Nov 2286 in seconds are epoch millis.
      Instant instant =
          Math.abs(epoch) >= 10_000_000_000L
              ? Instant.ofEpochMilli(epoch)
              : Instant.ofEpochSecond(epoch);
      return instant.atOffset(ZoneOffset.UTC);
    }
    if (!(value instanceof String raw)) {
      return null;
    }
    String s = raw.strip();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return OffsetDateTime.parse(s);
    } catch (DateTimeParseException ignored) {
      // not ISO-with-offset
    }
    String isoish = s.replace(' ', 'T');
    try {
      return LocalDateTime.parse(isoish).atOffset(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
      // not a naive datetime
    }
    try {
      return OffsetDateTime.parse(isoish);
    } catch (DateTimeParseException ignored) {
      // not a space-separated datetime with offset
    }
    try {
      return LocalDate.parse(s).atStartOfDay().atOffset(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
      return null;
    }
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
