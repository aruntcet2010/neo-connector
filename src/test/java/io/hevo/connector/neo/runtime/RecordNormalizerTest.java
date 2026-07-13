package io.hevo.connector.neo.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.hevo.connector.neo.manifest.ManifestLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * RecordNormalizer must emit exactly what the publisher's strict casts accept (HDatumUtils):
 * BOOLEAN → Boolean, LONG → Long, DOUBLE → Double, JSON/VARCHAR → String, DATE_TIME_TZ →
 * ISO-offset String, DATE → java.sql.Date.
 */
class RecordNormalizerTest {

  private static RecordNormalizer normalizer() {
    Map<String, Object> stream =
        ManifestLoader.load(
            """
            type: DeclarativeStream
            name: things
            retriever:
              type: SimpleRetriever
              requester:
                type: HttpRequester
                url_base: https://x.test
              record_selector:
                type: RecordSelector
                extractor:
                  type: DpathExtractor
                  field_path: []
            schema_loader:
              type: InlineSchemaLoader
              schema:
                type: object
                properties:
                  id: { type: integer }
                  amount: { type: number }
                  enabled: { type: boolean }
                  name: { type: string }
                  settings: { type: object }
                  updated_at: { type: string, format: date-time }
                  born_on: { type: string, format: date }
            """);
    return new RecordNormalizer(new StreamSpec(stream));
  }

  private static Map<String, Object> normalize(String field, Object value) {
    Map<String, Object> record = new HashMap<>();
    record.put(field, value);
    return normalizer().normalize(record);
  }

  @Test
  void booleanCoercion() {
    // strtobool semantics (Airbyte TypeTransformer parity)
    assertEquals(Boolean.TRUE, normalize("enabled", "yes").get("enabled"));
    assertEquals(Boolean.TRUE, normalize("enabled", "1").get("enabled"));
    assertEquals(Boolean.TRUE, normalize("enabled", "True").get("enabled"));
    assertEquals(Boolean.FALSE, normalize("enabled", "off").get("enabled"));
    // numeric 0/1 (e.g. ordergroove premier_enabled)
    assertEquals(Boolean.TRUE, normalize("enabled", 1).get("enabled"));
    assertEquals(Boolean.FALSE, normalize("enabled", 0).get("enabled"));
    assertEquals(Boolean.TRUE, normalize("enabled", true).get("enabled"));
    // unrecognized strings pass through (publisher will flag the record, not us)
    assertEquals("maybe", normalize("enabled", "maybe").get("enabled"));
  }

  @Test
  void dateTimeTzCanonicalizedToIsoOffset() {
    // space-separated naive (jotform style) → assumed UTC
    assertEquals(
        "2024-01-05T10:00:00Z", normalize("updated_at", "2024-01-05 10:00:00").get("updated_at"));
    // ISO naive
    assertEquals(
        "2024-01-05T10:00:00Z", normalize("updated_at", "2024-01-05T10:00:00").get("updated_at"));
    // already ISO with offset → stays parseable, offset preserved
    assertEquals(
        "2024-01-05T10:00:00+05:30",
        normalize("updated_at", "2024-01-05T10:00:00+05:30").get("updated_at"));
    // date-only → midnight UTC
    assertEquals("2024-01-05T00:00:00Z", normalize("updated_at", "2024-01-05").get("updated_at"));
    // epoch seconds and millis
    assertEquals(
        "2021-01-01T00:00:00Z", normalize("updated_at", 1609459200).get("updated_at"));
    assertEquals(
        "2021-01-01T00:00:00Z", normalize("updated_at", 1609459200000L).get("updated_at"));
    // garbage passes through with a warning
    assertEquals("not a date", normalize("updated_at", "not a date").get("updated_at"));
  }

  @Test
  void dateBecomesSqlDate() {
    Object date = normalize("born_on", "2024-01-05").get("born_on");
    assertInstanceOf(java.sql.Date.class, date);
    assertEquals(java.sql.Date.valueOf("2024-01-05"), date);
    // datetime string → its date part
    assertEquals(
        java.sql.Date.valueOf("2024-01-05"),
        normalize("born_on", "2024-01-05 10:00:00").get("born_on"));
  }

  @Test
  void numericAndJsonCoercions() {
    // integer-typed field arriving as other numeric shapes
    assertEquals(7L, normalize("id", 7).get("id"));
    assertEquals(7L, normalize("id", "7").get("id"));
    // number-typed field arriving as integer
    assertEquals(2.0, normalize("amount", 2).get("amount"));
    // object-typed field arriving parsed → JSON string
    assertEquals("{\"a\":1}", normalize("settings", Map.of("a", 1)).get("settings"));
    // string-typed field arriving as list → JSON string
    assertEquals("[1,2]", normalize("name", List.of(1, 2)).get("name"));
    assertEquals("42", normalize("name", 42).get("name"));
  }

  @Test
  void nullsAndUndeclaredFieldsUntouched() {
    Map<String, Object> record = new HashMap<>();
    record.put("enabled", null);
    record.put("extra_field", "kept-as-is");
    Map<String, Object> normalized = normalizer().normalize(record);
    assertEquals(null, normalized.get("enabled"));
    assertEquals("kept-as-is", normalized.get("extra_field"));
  }
}
