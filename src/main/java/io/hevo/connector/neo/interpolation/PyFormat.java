package io.hevo.connector.neo.interpolation;

import io.hevo.connector.neo.manifest.ManifestException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * Python strftime/strptime-compatible datetime formatting, mirroring the CDK's DatetimeParser
 * (airbyte_cdk/sources/declarative/datetime/datetime_parser.py) including its special epoch
 * formats: {@code %s} (epoch seconds), {@code %s_as_float}, {@code %ms} (epoch millis), {@code
 * %epoch_microseconds}, and {@code %_ms} (milliseconds field, zero-padded to 3).
 */
public final class PyFormat {

  private PyFormat() {}

  public static String format(ZonedDateTime dt, String format) {
    ZonedDateTime utc = dt.withZoneSameInstant(ZoneOffset.UTC);
    switch (format) {
      case "%s":
        return String.valueOf(utc.toEpochSecond());
      case "%s_as_float":
        double seconds = utc.toEpochSecond() + utc.getNano() / 1_000_000_000.0;
        return String.valueOf(seconds);
      case "%ms":
        return String.valueOf(utc.toInstant().toEpochMilli());
      case "%epoch_microseconds":
        return String.valueOf(utc.toEpochSecond() * 1_000_000L + utc.getNano() / 1_000L);
      default:
        return toFormatter(format.replace("%_ms", "%f3")).format(dt);
    }
  }

  public static ZonedDateTime parse(String value, String format) {
    switch (format) {
      case "%s":
        return Instant.ofEpochSecond(Long.parseLong(value.trim())).atZone(ZoneOffset.UTC);
      case "%s_as_float":
        double seconds = Double.parseDouble(value.trim());
        long whole = (long) seconds;
        long nanos = Math.round((seconds - whole) * 1_000_000_000L);
        return Instant.ofEpochSecond(whole, nanos).atZone(ZoneOffset.UTC);
      case "%ms":
        return Instant.ofEpochMilli(Long.parseLong(value.trim())).atZone(ZoneOffset.UTC);
      case "%epoch_microseconds":
        long micros = Long.parseLong(value.trim());
        return Instant.ofEpochSecond(micros / 1_000_000L, (micros % 1_000_000L) * 1_000L)
            .atZone(ZoneOffset.UTC);
      default:
        DateTimeFormatter formatter = toFormatter(format.replace("%_ms", "%f3"));
        try {
          TemporalAccessor parsed = formatter.parse(value);
          if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
            return OffsetDateTime.from(parsed).toZonedDateTime();
          }
          if (parsed.isSupported(ChronoField.HOUR_OF_DAY)) {
            // Naive datetimes are assumed UTC, matching the CDK.
            return LocalDateTime.from(parsed).atZone(ZoneOffset.UTC);
          }
          return LocalDate.from(parsed).atStartOfDay(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
          throw new ManifestException(
              "Cannot parse '" + value + "' with format '" + format + "'", e);
        }
    }
  }

  /** ISO-8601 parse equivalent to dateutil.isoparse; naive values are assumed UTC. */
  public static ZonedDateTime parseIso(String value) {
    try {
      return OffsetDateTime.parse(value).toZonedDateTime();
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return LocalDateTime.parse(value).atZone(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC);
    } catch (DateTimeParseException e) {
      throw new ManifestException("Cannot parse datetime: " + value, e);
    }
  }

  private static DateTimeFormatter toFormatter(String format) {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    int i = 0;
    while (i < format.length()) {
      char c = format.charAt(i);
      if (c != '%') {
        builder.appendLiteral(c);
        i++;
        continue;
      }
      if (i + 1 >= format.length()) {
        throw new ManifestException("Trailing '%' in datetime format: " + format);
      }
      char directive = format.charAt(i + 1);
      i += 2;
      switch (directive) {
        case 'Y' -> builder.appendPattern("uuuu");
        case 'y' -> builder.appendPattern("uu");
        case 'm' -> builder.appendPattern("MM");
        case 'd' -> builder.appendPattern("dd");
        case 'H' -> builder.appendPattern("HH");
        case 'I' -> builder.appendPattern("hh");
        case 'M' -> builder.appendPattern("mm");
        case 'S' -> builder.appendPattern("ss");
        case 'f' -> {
          // %f3 is our internal rewrite of the CDK's %_ms (3-digit milliseconds).
          if (i < format.length() && format.charAt(i) == '3') {
            i++;
            builder.appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, false);
          } else {
            builder.appendFraction(ChronoField.MICRO_OF_SECOND, 6, 6, false);
          }
        }
        case 'z' -> builder.appendOffset("+HHmm", "+0000");
        case 'Z' -> builder.appendPattern("zzz");
        case 'j' -> builder.appendPattern("DDD");
        case 'a' -> builder.appendPattern("EEE");
        case 'A' -> builder.appendPattern("EEEE");
        case 'b' -> builder.appendPattern("MMM");
        case 'B' -> builder.appendPattern("MMMM");
        case 'p' -> builder.appendPattern("a");
        case '%' -> builder.appendLiteral('%');
        default -> throw new ManifestException(
            "Unsupported strftime directive %" + directive + " in " + format);
      }
    }
    return builder.toFormatter(Locale.ENGLISH);
  }
}
