package io.hevo.connector.neo.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.hevo.connector.neo.manifest.ManifestLoader;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DatetimeCursorTest {

  private static DatetimeCursor cursor(String yaml, Map<String, Object> config) {
    return new DatetimeCursor(ManifestLoader.load(yaml), config);
  }

  private static final String WINDOWED =
      """
            type: DatetimeBasedCursor
            cursor_field: updated_at
            start_datetime:
              type: MinMaxDatetime
              datetime: '{{ config["start_date"] }}'
              datetime_format: "%Y-%m-%dT%H:%M:%SZ"
            end_datetime:
              type: MinMaxDatetime
              datetime: '{{ config["end_date"] }}'
              datetime_format: "%Y-%m-%dT%H:%M:%SZ"
            datetime_format: "%Y-%m-%d %H:%M:%S"
            step: P10D
            cursor_granularity: PT1S
            start_time_option:
              type: RequestOption
              inject_into: request_parameter
              field_name: modified_since
            """;

  @Test
  void windowsAdvanceByStepAndEndAtGranularityBoundary() {
    DatetimeCursor c =
        cursor(
            WINDOWED,
            Map.of(
                "start_date", "2024-01-01T00:00:00Z",
                "end_date", "2024-01-25T00:00:00Z"));
    List<Map<String, Object>> windows = c.windows();
    assertEquals(3, windows.size());
    // Window ends are next_start - cursor_granularity (1s), matching the CDK formula.
    assertEquals("2024-01-01 00:00:00", windows.get(0).get("start_time"));
    assertEquals("2024-01-10 23:59:59", windows.get(0).get("end_time"));
    assertEquals("2024-01-11 00:00:00", windows.get(1).get("start_time"));
    assertEquals("2024-01-20 23:59:59", windows.get(1).get("end_time"));
    // Final window clamps to the configured end.
    assertEquals("2024-01-25 00:00:00", windows.get(2).get("end_time"));
  }

  @Test
  void noStepMeansSingleWindow() {
    DatetimeCursor c =
        cursor(
            """
                        type: DatetimeBasedCursor
                        cursor_field: updated_at
                        start_datetime:
                          type: MinMaxDatetime
                          datetime: '{{ config["start_date"] }}'
                          datetime_format: "%Y-%m-%dT%H:%M:%SZ"
                        datetime_format: "%Y-%m-%d %H:%M:%S"
                        """,
            Map.of("start_date", "2024-01-01T00:00:00Z"));
    assertEquals(1, c.windows().size());
  }

  @Test
  void stateAdvancesStartAndSurvivesRoundTrip() {
    DatetimeCursor c =
        cursor(
            WINDOWED,
            Map.of(
                "start_date", "2024-01-01T00:00:00Z",
                "end_date", "2024-01-25T00:00:00Z"));
    c.setInitialState("2024-01-20 12:00:00");
    List<Map<String, Object>> windows = c.windows();
    // State cursor is later than configured start → start from the state value.
    assertEquals("2024-01-20 12:00:00", windows.get(0).get("start_time"));
    assertEquals(1, windows.size());
  }

  @Test
  void observeTracksMaxWithinWindowOnly() {
    DatetimeCursor c =
        cursor(
            WINDOWED,
            Map.of(
                "start_date", "2024-01-01T00:00:00Z",
                "end_date", "2024-01-25T00:00:00Z"));
    Map<String, Object> window =
        Map.of("start_time", "2024-01-01 00:00:00", "end_time", "2024-01-10 23:59:59");
    assertNull(c.state());
    c.observe(Map.of("updated_at", "2024-01-05 08:00:00"), window);
    c.observe(Map.of("updated_at", "2024-01-03 08:00:00"), window); // lower, ignored
    c.observe(Map.of("updated_at", "2024-02-01 08:00:00"), window); // outside window, ignored
    assertEquals("2024-01-05 08:00:00", c.state());
    // Previous state higher than everything observed wins.
    c.setInitialState("2024-01-09 00:00:00");
    assertEquals("2024-01-09 00:00:00", c.state());
  }

  @Test
  void requestOptionsInjectWindowBounds() {
    DatetimeCursor c =
        cursor(
            WINDOWED,
            Map.of(
                "start_date", "2024-01-01T00:00:00Z",
                "end_date", "2024-01-25T00:00:00Z"));
    Map<String, Object> window = c.windows().get(0);
    Map<String, Object> params =
        c.requestOptions(RequestOptionSpec.InjectInto.REQUEST_PARAMETER, window);
    assertEquals("2024-01-01 00:00:00", params.get("modified_since"));
    assertTrue(c.requestOptions(RequestOptionSpec.InjectInto.HEADER, window).isEmpty());
  }
}
