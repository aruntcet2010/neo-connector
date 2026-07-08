package io.hevo.connector.neo.interpolation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.hevo.connector.neo.manifest.ManifestException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JinjaEngineTest {

    private final JinjaEngine engine = new JinjaEngine();

    private Object eval(String template, Map<String, Object> context) {
        return engine.eval(template, context);
    }

    // --- basic interpolation -------------------------------------------------

    @Test
    void plainStringPassesThrough() {
        assertEquals("hello world", eval("hello world", Map.of("config", Map.of())));
    }

    @Test
    void configAccessBothStyles() {
        Map<String, Object> context = Map.of("config", Map.of("name", "airbyte"));
        assertEquals("hello airbyte", eval("hello {{ config.name }}", context));
        assertEquals("hello airbyte", eval("hello {{ config['name'] }}", context));
    }

    @Test
    void streamIntervalAliasesStreamSlice() {
        Map<String, Object> context =
                Map.of(
                        "config", Map.of(),
                        "stream_slice", Map.of("start_time", "2024-01-01"));
        assertEquals("2024-01-01", eval("{{ stream_interval['start_time'] }}", context));
        assertEquals("2024-01-01", eval("{{ stream_partition['start_time'] }}", context));
    }

    @Test
    void streamStateIsRejected() {
        assertThrows(
                ManifestException.class,
                () -> eval("{{ stream_state['x'] }}", Map.of("config", Map.of())));
    }

    // --- literal_eval coercion (verified against Python ast.literal_eval) ----

    @Test
    void coercionMatchesPythonLiteralEval() {
        Map<String, Object> context = Map.of("config", Map.of());
        assertEquals(3L, eval("{{ '3' }}", context));
        assertEquals("03", eval("{{ '03' }}", context)); // leading zero stays string
        assertEquals(3.5, eval("{{ '3.5' }}", context));
        assertEquals(1000L, eval("{{ '1_000' }}", context));
        assertEquals(Boolean.TRUE, eval("{{ 'True' }}", context));
        assertEquals("true", eval("{{ 'true' }}", context)); // lowercase stays string
        assertEquals("2023-01-01", eval("{{ '2023-01-01' }}", context));
        assertEquals(Map.of("a", 1), eval("{{ \"{'a': 1}\" }}", context));
        assertEquals(List.of(1, 2), eval("{{ '[1, 2]' }}", context));
    }

    @Test
    void nativeTypesSurviveSingleExpressions() {
        Map<String, Object> context =
                Map.of("config", Map.of("count", 5, "opts", Map.of("k", "v")));
        assertEquals(5, eval("{{ config['count'] }}", context));
        assertEquals(Map.of("k", "v"), eval("{{ config['opts'] }}", context));
    }

    @Test
    void emptyResultFallsBackToDefault() {
        // Missing keys render empty; with default == template the raw render ("") comes back.
        assertEquals("", eval("{{ config['missing'] }}", Map.of("config", Map.of())));
        // Explicit default template
        assertEquals(
                10L,
                engine.eval("{{ config['missing'] }}", Map.of("config", Map.of()), "{{ 10 }}"));
    }

    // --- macros ---------------------------------------------------------------

    @Test
    void nowUtcSupportsStrftime() {
        Object result =
                eval("{{ now_utc().strftime('%Y-%m-%d') }}", Map.of("config", Map.of()));
        assertInstanceOf(String.class, result);
        assertTrue(result.toString().matches("\\d{4}-\\d{2}-\\d{2}"), "got: " + result);
    }

    @Test
    void timestampMacro() {
        Map<String, Object> context = Map.of("config", Map.of());
        assertEquals(1609459200L, eval("{{ timestamp(1609459200) }}", context));
        assertEquals(1609459200.0, eval("{{ timestamp('2021-01-01T00:00:00Z') }}", context));
    }

    @Test
    void formatDatetimeMacro() {
        Map<String, Object> context = Map.of("config", Map.of("start", "2021-01-01T05:06:07Z"));
        assertEquals(
                "2021-01-01",
                eval("{{ format_datetime(config['start'], '%Y-%m-%d') }}", context));
        assertEquals(
                "1609477567",
                eval("{{ format_datetime(config['start'], '%s') }}", context).toString());
    }

    @Test
    void maxMinAndDuration() {
        Map<String, Object> context = Map.of("config", Map.of());
        assertEquals(3L, eval("{{ max(2, 3) }}", context));
        assertEquals(2L, eval("{{ min(2, 3) }}", context));
        assertEquals("PT24H", eval("{{ duration('P1D') }}", context));
    }

    // --- filters ----------------------------------------------------------------

    @Test
    void base64AndHashFilters() {
        Map<String, Object> context = Map.of("config", Map.of("token", "hello"));
        assertEquals("aGVsbG8=", eval("{{ config['token'] | base64encode }}", context));
        assertEquals(
                "5d41402abc4b2a76b9719d911017c592",
                eval("{{ config['token'] | hash('md5') }}", context));
    }

    @Test
    void regexFilters() {
        Map<String, Object> context =
                Map.of("config", Map.of("link", "<https://x.test/page?p=2>; rel=\"next\""));
        assertEquals(
                "https://x.test/page?p=2",
                eval("{{ config['link'] | regex_search('<(.*)>;') }}", context));
    }

    @Test
    void nullTemplateIsNull() {
        assertNull(engine.eval(null, Map.of("config", Map.of())));
    }
}
