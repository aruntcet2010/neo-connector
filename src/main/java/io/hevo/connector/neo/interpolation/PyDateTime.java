package io.hevo.connector.neo.interpolation;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Wrapper around {@link ZonedDateTime} exposing the Python datetime methods manifests call in
 * templates (e.g. {@code {{ now_utc().strftime('%Y-%m-%d') }}}). {@link #toString()} matches
 * Python's {@code str(datetime)} so plain {@code {{ now_utc() }}} renders identically.
 */
public final class PyDateTime implements Comparable<PyDateTime> {

    private final ZonedDateTime value;

    public PyDateTime(ZonedDateTime value) {
        this.value = value;
    }

    public ZonedDateTime toZonedDateTime() {
        return value;
    }

    public String strftime(String format) {
        return PyFormat.format(value, format);
    }

    public double timestamp() {
        return value.toEpochSecond() + value.getNano() / 1_000_000_000.0;
    }

    public String isoformat() {
        // Python: 2024-01-02T03:04:05.123456+00:00 (no fraction when zero)
        String pattern =
                value.getNano() == 0 ? "yyyy-MM-dd'T'HH:mm:ssxxx" : "yyyy-MM-dd'T'HH:mm:ss.SSSSSSxxx";
        return value.format(DateTimeFormatter.ofPattern(pattern));
    }

    @Override
    public String toString() {
        // Python str(datetime): "2024-01-02 03:04:05.123456+00:00" (no fraction when zero)
        String pattern =
                value.getNano() == 0 ? "yyyy-MM-dd HH:mm:ssxxx" : "yyyy-MM-dd HH:mm:ss.SSSSSSxxx";
        return value.format(DateTimeFormatter.ofPattern(pattern));
    }

    @Override
    public int compareTo(PyDateTime other) {
        return value.toInstant().compareTo(other.value.toInstant());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PyDateTime p && value.toInstant().equals(p.value.toInstant());
    }

    @Override
    public int hashCode() {
        return value.toInstant().hashCode();
    }

    /** Python str(date): "2024-01-02". */
    public record PyDate(LocalDate value) {

        public String strftime(String format) {
            return PyFormat.format(value.atStartOfDay(ZoneOffset.UTC), format);
        }

        public String isoformat() {
            return value.toString();
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
