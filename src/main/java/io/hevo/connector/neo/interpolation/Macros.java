package io.hevo.connector.neo.interpolation;

import io.hevo.connector.neo.manifest.ManifestException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * Global template functions, mirroring airbyte_cdk/sources/declarative/interpolation/macros.py.
 * All methods are public static so they can be registered as Jinjava EL functions.
 */
public final class Macros {

    private Macros() {}

    public static PyDateTime nowUtc() {
        return new PyDateTime(ZonedDateTime.now(ZoneOffset.UTC));
    }

    public static PyDateTime.PyDate todayUtc() {
        return new PyDateTime.PyDate(LocalDate.now(ZoneOffset.UTC));
    }

    public static PyDateTime.PyDate todayWithTimezone(String timezone) {
        return new PyDateTime.PyDate(LocalDate.now(ZoneId.of(timezone)));
    }

    /** Numeric input → epoch seconds as long; string input → parsed ISO datetime's epoch (double). */
    public static Object timestamp(Object dt) {
        if (dt instanceof Number n) {
            return n.longValue();
        }
        String s = String.valueOf(dt);
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException ignored) {
            // not numeric; fall through to datetime parsing
        }
        ZonedDateTime parsed = PyFormat.parseIso(s);
        return parsed.toEpochSecond() + parsed.getNano() / 1_000_000_000.0;
    }

    public static PyDateTime strToDatetime(String s) {
        return new PyDateTime(PyFormat.parseIso(s).withZoneSameInstant(ZoneOffset.UTC));
    }

    public static Object max(Object... args) {
        return reduce(args, true);
    }

    public static Object min(Object... args) {
        return reduce(args, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object reduce(Object[] args, boolean wantMax) {
        Object[] values = args;
        if (args.length == 1 && args[0] instanceof java.util.Collection<?> collection) {
            values = collection.toArray();
        }
        if (values.length == 0) {
            throw new ManifestException("max()/min() requires at least one argument");
        }
        Object best = values[0];
        for (int i = 1; i < values.length; i++) {
            int cmp;
            if (best instanceof Number a && values[i] instanceof Number b) {
                cmp = Double.compare(a.doubleValue(), b.doubleValue());
            } else if (best instanceof Comparable comparable) {
                cmp = comparable.compareTo(values[i]);
            } else {
                throw new ManifestException("max()/min(): values are not comparable");
            }
            if (wantMax ? cmp < 0 : cmp > 0) {
                best = values[i];
            }
        }
        return best;
    }

    /** now (UTC) + numDays days, formatted; default format matches the Python macro. */
    public static String dayDelta(Object numDays, String... format) {
        String fmt = format.length > 0 && format[0] != null ? format[0] : "%Y-%m-%dT%H:%M:%S.%f%z";
        long days = ((Number) coerceNumber(numDays)).longValue();
        return PyFormat.format(ZonedDateTime.now(ZoneOffset.UTC).plusDays(days), fmt);
    }

    /** ISO-8601 duration; day/time durations → {@link Duration}, calendar ones → {@link Period}. */
    public static Object duration(String datestring) {
        try {
            return Duration.parse(datestring);
        } catch (RuntimeException ignored) {
            // Durations with date components (e.g. P1M) are not Duration-parsable.
        }
        try {
            return Period.parse(datestring);
        } catch (RuntimeException e) {
            throw new ManifestException("Cannot parse ISO8601 duration: " + datestring, e);
        }
    }

    public static String formatDatetime(Object dt, String format, String... inputFormat) {
        String input = inputFormat.length > 0 ? inputFormat[0] : null;
        ZonedDateTime value;
        if (dt instanceof PyDateTime pyDateTime) {
            value = pyDateTime.toZonedDateTime();
        } else if (dt instanceof ZonedDateTime zonedDateTime) {
            value = zonedDateTime;
        } else if (dt instanceof Number n) {
            value = PyFormat.parse(String.valueOf(n.longValue()), input != null ? input : "%s");
        } else {
            String s = String.valueOf(dt);
            value = input != null ? PyFormat.parse(s, input) : PyFormat.parseIso(s);
        }
        return PyFormat.format(value.withZoneSameInstant(ZoneOffset.UTC), format);
    }

    public static String sanitizeUrl(Object value) {
        return URLEncoder.encode(String.valueOf(value), StandardCharsets.UTF_8);
    }

    public static String camelCaseToSnakeCase(String value) {
        return value.replaceAll("(?<!^)(?=[A-Z])", "_").toLowerCase();
    }

    public static String generateUuid() {
        return UUID.randomUUID().toString();
    }

    private static Object coerceNumber(Object value) {
        if (value instanceof Number) {
            return value;
        }
        return Long.parseLong(String.valueOf(value).trim());
    }
}
