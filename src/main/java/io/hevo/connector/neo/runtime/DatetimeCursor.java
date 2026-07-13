package io.hevo.connector.neo.runtime;

import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.interpolation.PyFormat;
import io.hevo.connector.neo.manifest.ManifestException;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DatetimeBasedCursor, mirroring the CDK's windowing and state semantics
 * (legacy/sources/declarative/incremental/datetime_based_cursor.py):
 *
 * <ul>
 *   <li>end = min(configured end_datetime, now); start = max(min(start_datetime, end), state_cursor
 *       − lookback_window).
 *   <li>Windows advance by {@code step}; each window ends at {@code min(next_start −
 *       cursor_granularity, end)}. No step → one window.
 *   <li>{@code observe} tracks the highest record cursor value within the window's bounds; closing
 *       takes the max of state and the highest observed, preserving the record's original string
 *       representation.
 *   <li>{@code start_time_option}/{@code end_time_option} inject the window bounds into requests.
 * </ul>
 *
 * <p>P2 note: one global cursor across partitions (Hevo parent-level checkpointing), not Airbyte's
 * per-partition state.
 */
public final class DatetimeCursor {

  private static final Logger log = LoggerFactory.getLogger(DatetimeCursor.class);
  private static final String DEFAULT_MINMAX_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z";

  private final Map<String, Object> config;
  private final String cursorField;
  private final String datetimeFormat;
  private final List<String> parseFormats;
  private final Map<String, Object> startDatetime;
  private final Map<String, Object> endDatetime;
  private final TemporalAmount step;
  private final Duration cursorGranularity;
  private final Duration lookbackWindow;
  private final RequestOptionSpec startTimeOption;
  private final RequestOptionSpec endTimeOption;
  private final String partitionFieldStart;
  private final String partitionFieldEnd;
  private final boolean isCompareStrictly;

  private String stateCursorValue;
  private String highestObservedValue;

  public DatetimeCursor(Map<String, Object> component, Map<String, Object> config) {
    this.config = config;
    Map<String, Object> parameters = Components.parameters(component);
    String field = Components.getString(component, "cursor_field");
    if (field == null) {
      throw new ManifestException("DatetimeBasedCursor requires cursor_field");
    }
    this.cursorField = String.valueOf(InterpolatedString.create(field, parameters).eval(config));
    this.datetimeFormat = Components.getString(component, "datetime_format");
    if (datetimeFormat == null) {
      throw new ManifestException("DatetimeBasedCursor requires datetime_format");
    }
    List<Object> formats = Components.getList(component, "cursor_datetime_formats");
    this.parseFormats = new ArrayList<>();
    if (formats != null) {
      formats.forEach(f -> parseFormats.add(String.valueOf(f)));
    }
    if (!parseFormats.contains(datetimeFormat)) {
      parseFormats.add(datetimeFormat);
    }
    this.startDatetime = Components.getMap(component, "start_datetime");
    this.endDatetime = Components.getMap(component, "end_datetime");
    String stepString = Components.getString(component, "step");
    String granularity = Components.getString(component, "cursor_granularity");
    if ((stepString == null) != (granularity == null)) {
      throw new ManifestException("step and cursor_granularity must be provided together");
    }
    this.step = stepString == null ? null : parseTemporalAmount(stepString);
    this.cursorGranularity = granularity == null ? Duration.ZERO : Duration.parse(granularity);
    String lookback = Components.getString(component, "lookback_window");
    this.lookbackWindow = lookback == null ? Duration.ZERO : parseLookback(lookback);
    this.startTimeOption =
        RequestOptionSpec.from(Components.getMap(component, "start_time_option"));
    this.endTimeOption = RequestOptionSpec.from(Components.getMap(component, "end_time_option"));
    this.partitionFieldStart =
        orDefault(Components.getString(component, "partition_field_start"), "start_time");
    this.partitionFieldEnd =
        orDefault(Components.getString(component, "partition_field_end"), "end_time");
    this.isCompareStrictly = Boolean.TRUE.equals(component.get("is_compare_strictly"));
  }

  private static String orDefault(String value, String fallback) {
    return value == null || value.isEmpty() ? fallback : value;
  }

  public String cursorField() {
    return cursorField;
  }

  public void setInitialState(String cursorValue) {
    this.stateCursorValue = cursorValue;
  }

  /** Cursor windows as {start_field: ..., end_field: ...} maps in the cursor's format. */
  public List<Map<String, Object>> windows() {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    ZonedDateTime end = evalMinMax(endDatetime, now);
    if (end == null || end.isAfter(now)) {
      end = now;
    }
    ZonedDateTime configuredStart = evalMinMax(startDatetime, null);
    if (configuredStart == null) {
      throw new ManifestException("DatetimeBasedCursor requires start_datetime");
    }
    ZonedDateTime earliest = configuredStart.isAfter(end) ? end : configuredStart;
    if (stateCursorValue != null) {
      ZonedDateTime fromState = parse(stateCursorValue).minus(lookbackWindow);
      if (fromState.isAfter(earliest)) {
        earliest = fromState;
      }
    }
    List<Map<String, Object>> windows = new ArrayList<>();
    ZonedDateTime start = earliest;
    if (step == null) {
      windows.add(window(start, end));
      return windows;
    }
    while (isCompareStrictly ? start.isBefore(end) : !start.isAfter(end)) {
      ZonedDateTime nextStart = start.plus(step);
      ZonedDateTime windowEnd = nextStart.minus(cursorGranularity);
      if (windowEnd.isAfter(end)) {
        windowEnd = end;
      }
      windows.add(window(start, windowEnd));
      start = nextStart;
    }
    return windows;
  }

  private Map<String, Object> window(ZonedDateTime start, ZonedDateTime end) {
    Map<String, Object> window = new LinkedHashMap<>();
    window.put(partitionFieldStart, PyFormat.format(start, datetimeFormat));
    window.put(partitionFieldEnd, PyFormat.format(end, datetimeFormat));
    return window;
  }

  /** Track the highest in-window cursor value, keeping its original string form. */
  public void observe(Object record, Map<String, Object> window) {
    if (!(record instanceof Map<?, ?> map)) {
      return;
    }
    Object rawValue = map.get(cursorField);
    if (rawValue == null) {
      return;
    }
    String value = String.valueOf(rawValue);
    ZonedDateTime parsed;
    try {
      parsed = parse(value);
    } catch (RuntimeException e) {
      log.warn("Record cursor value not parseable: {}", value);
      return;
    }
    ZonedDateTime windowStart = parse(String.valueOf(window.get(partitionFieldStart)));
    ZonedDateTime windowEnd = parse(String.valueOf(window.get(partitionFieldEnd)));
    if (parsed.isBefore(windowStart) || parsed.isAfter(windowEnd)) {
      return;
    }
    if (highestObservedValue == null || parsed.isAfter(parse(highestObservedValue))) {
      highestObservedValue = value;
    }
  }

  /** Final cursor value: max(previous state, highest observed), or null if neither. */
  public String state() {
    if (highestObservedValue == null) {
      return stateCursorValue;
    }
    if (stateCursorValue == null || parse(highestObservedValue).isAfter(parse(stateCursorValue))) {
      return highestObservedValue;
    }
    return stateCursorValue;
  }

  /** start/end_time_option injections for a window. */
  public Map<String, Object> requestOptions(
      RequestOptionSpec.InjectInto target, Map<String, Object> window) {
    Map<String, Object> options = new LinkedHashMap<>();
    addOption(options, startTimeOption, target, window.get(partitionFieldStart));
    addOption(options, endTimeOption, target, window.get(partitionFieldEnd));
    return options;
  }

  private void addOption(
      Map<String, Object> options,
      RequestOptionSpec option,
      RequestOptionSpec.InjectInto target,
      Object value) {
    if (option != null
        && option.injectInto() == target
        && option.fieldName() != null
        && value != null) {
      options.put(String.valueOf(option.fieldName().eval(config)), value);
    }
  }

  private ZonedDateTime parse(String value) {
    RuntimeException last = null;
    for (String format : parseFormats) {
      try {
        return PyFormat.parse(value, format);
      } catch (RuntimeException e) {
        last = e;
      }
    }
    try {
      return PyFormat.parseIso(value);
    } catch (RuntimeException e) {
      throw last != null ? last : e;
    }
  }

  /** Evaluates a MinMaxDatetime component: interpolate, parse, clamp to min/max. */
  private ZonedDateTime evalMinMax(Map<String, Object> component, ZonedDateTime fallback) {
    if (component == null) {
      return fallback;
    }
    Map<String, Object> parameters = Components.parameters(component);
    String format = Components.getString(component, "datetime_format");
    String effectiveFormat = format == null || format.isEmpty() ? DEFAULT_MINMAX_FORMAT : format;
    ZonedDateTime value =
        parseMinMaxValue(Components.getString(component, "datetime"), effectiveFormat, parameters);
    if (value == null) {
      return fallback;
    }
    ZonedDateTime min =
        parseMinMaxValue(
            Components.getString(component, "min_datetime"), effectiveFormat, parameters);
    ZonedDateTime max =
        parseMinMaxValue(
            Components.getString(component, "max_datetime"), effectiveFormat, parameters);
    if (min != null && value.isBefore(min)) {
      value = min;
    }
    if (max != null && value.isAfter(max)) {
      value = max;
    }
    return value;
  }

  private ZonedDateTime parseMinMaxValue(
      String template, String format, Map<String, Object> parameters) {
    if (template == null) {
      return null;
    }
    Object evaluated = InterpolatedString.create(template, parameters).eval(config);
    String value = evaluated == null ? "" : String.valueOf(evaluated);
    if (value.isEmpty() || value.contains("{{")) {
      return null;
    }
    try {
      return PyFormat.parse(value, format);
    } catch (RuntimeException e) {
      return PyFormat.parseIso(value);
    }
  }

  /** ISO-8601 durations, including calendar parts (P1M) via Period. */
  private static TemporalAmount parseTemporalAmount(String iso) {
    try {
      return Duration.parse(iso);
    } catch (RuntimeException ignored) {
      return Period.parse(iso);
    }
  }

  private static Duration parseLookback(String iso) {
    try {
      return Duration.parse(iso);
    } catch (RuntimeException ignored) {
      // Calendar-based lookbacks approximate: P1D = 24h, P1W = 7d, P1M = 30d, P1Y = 365d.
      Period period = Period.parse(iso);
      return Duration.ofDays(
          period.getDays() + period.getMonths() * 30L + period.getYears() * 365L);
    }
  }
}
