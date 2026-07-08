package io.hevo.connector.neo.interpolation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition;
import io.hevo.connector.neo.manifest.ManifestException;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Jinja template evaluation with the semantics of the CDK's JinjaInterpolation
 * (airbyte_cdk/sources/declarative/interpolation/jinja.py):
 *
 * <ul>
 *   <li>{@code stream_interval} and {@code stream_partition} alias {@code stream_slice}.
 *   <li>{@code stream_state} in a template is rejected (removed from the CDK).
 *   <li>An empty/undefined evaluation falls back to the default (itself a template).
 *   <li>Rendered strings are coerced like Python's {@code ast.literal_eval}: {@code "3"} → long,
 *       {@code "3.5"} → double, {@code "True"/"False"} → boolean, {@code "None"} → null,
 *       dict/list literals → Map/List; anything else (including {@code "03"}, {@code "true"})
 *       stays a string.
 *   <li>A template that is a single {@code {{ expr }}} expression is resolved natively so
 *       non-string values (maps, lists, numbers) survive without a string round-trip.
 * </ul>
 */
public final class JinjaEngine {

    private static final Map<String, String> ALIASES =
            Map.of(
                    "stream_interval", "stream_slice",
                    "stream_partition", "stream_slice");

    private static final Pattern INTEGER = Pattern.compile("[+-]?\\d+(?:_\\d+)*");
    private static final Pattern FLOAT =
            Pattern.compile("[+-]?(?:\\d+\\.\\d*|\\.\\d+|\\d+(?:\\.\\d*)?[eE][+-]?\\d+)");
    private static final ObjectMapper LENIENT_JSON =
            new ObjectMapper()
                    .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                    .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    private static final Jinjava JINJAVA = buildJinjava();

    private static Jinjava buildJinjava() {
        Jinjava jinjava = new Jinjava();
        Context context = jinjava.getGlobalContext();
        context.registerFunction(new ELFunctionDefinition("", "now_utc", Macros.class, "nowUtc"));
        context.registerFunction(
                new ELFunctionDefinition("", "today_utc", Macros.class, "todayUtc"));
        context.registerFunction(
                new ELFunctionDefinition(
                        "", "today_with_timezone", Macros.class, "todayWithTimezone",
                        String.class));
        context.registerFunction(
                new ELFunctionDefinition("", "timestamp", Macros.class, "timestamp", Object.class));
        context.registerFunction(
                new ELFunctionDefinition(
                        "", "str_to_datetime", Macros.class, "strToDatetime", String.class));
        context.registerFunction(
                new ELFunctionDefinition("", "max", Macros.class, "max", Object[].class));
        context.registerFunction(
                new ELFunctionDefinition("", "min", Macros.class, "min", Object[].class));
        context.registerFunction(
                new ELFunctionDefinition(
                        "", "day_delta", Macros.class, "dayDelta", Object.class, String[].class));
        context.registerFunction(
                new ELFunctionDefinition("", "duration", Macros.class, "duration", String.class));
        context.registerFunction(
                new ELFunctionDefinition(
                        "", "format_datetime", Macros.class, "formatDatetime",
                        Object.class, String.class, String[].class));
        context.registerFunction(
                new ELFunctionDefinition(
                        "", "sanitize_url", Macros.class, "sanitizeUrl", Object.class));
        context.registerFunction(
                new ELFunctionDefinition(
                        "", "camel_case_to_snake_case", Macros.class, "camelCaseToSnakeCase",
                        String.class));
        context.registerFunction(
                new ELFunctionDefinition("", "generate_uuid", Macros.class, "generateUuid"));
        for (Filter filter : Filters.all()) {
            context.registerFilter(filter);
        }
        return jinjava;
    }

    public Object eval(String template, Map<String, Object> context) {
        return eval(template, context, template);
    }

    public Object eval(String template, Map<String, Object> context, String defaultTemplate) {
        if (template == null) {
            return null;
        }
        Map<String, Object> bindings = withAliases(context);
        if (template.contains("stream_state")) {
            throw new ManifestException(
                    "`stream_state` is no longer supported for interpolation. Use"
                            + " `stream_interval` instead.");
        }
        Object result = evalOnce(template, bindings);
        if (result != null && !"".equals(result)) {
            return result;
        }
        if (defaultTemplate == null || defaultTemplate.equals(template)) {
            Object rendered = render(defaultTemplate == null ? "" : defaultTemplate, bindings);
            return literalEval((String) rendered);
        }
        Object defaultResult = evalOnce(defaultTemplate, bindings);
        return defaultResult == null ? "" : defaultResult;
    }

    /** Evaluates a template once: native resolution for single expressions, render otherwise. */
    private Object evalOnce(String template, Map<String, Object> bindings) {
        String expression = singleExpression(template);
        if (expression != null) {
            Object value = resolveExpression(expression, bindings);
            if (value == null) {
                return null;
            }
            if (value instanceof String
                    || value instanceof Number
                    || value instanceof Boolean
                    || value instanceof Map
                    || value instanceof List) {
                return value instanceof String s ? literalEval(s) : value;
            }
            // Other object types (dates, durations) round-trip through their string form the
            // way Python renders them.
            return literalEval(String.valueOf(value));
        }
        String rendered = render(template, bindings);
        if (rendered == null || rendered.isEmpty()) {
            return null;
        }
        return literalEval(rendered);
    }

    private String render(String template, Map<String, Object> bindings) {
        return JINJAVA.render(template, bindings);
    }

    private Object resolveExpression(String expression, Map<String, Object> bindings) {
        JinjavaInterpreter interpreter =
                new JinjavaInterpreter(
                        JINJAVA,
                        new Context(JINJAVA.getGlobalContext(), bindings),
                        JINJAVA.getGlobalConfig());
        JinjavaInterpreter.pushCurrent(interpreter);
        try {
            return interpreter.resolveELExpression(expression, -1);
        } finally {
            JinjavaInterpreter.popCurrent();
        }
    }

    /** Returns the inner expression when the template is exactly one {{ ... }}, else null. */
    private static String singleExpression(String template) {
        String trimmed = template.trim();
        if (!trimmed.startsWith("{{") || !trimmed.endsWith("}}") || trimmed.length() < 4) {
            return null;
        }
        String inner = trimmed.substring(2, trimmed.length() - 2);
        if (inner.contains("{{") || inner.contains("}}")) {
            return null;
        }
        return inner;
    }

    private static Map<String, Object> withAliases(Map<String, Object> context) {
        Map<String, Object> bindings = new LinkedHashMap<>(context);
        for (Map.Entry<String, String> alias : ALIASES.entrySet()) {
            if (context.containsKey(alias.getKey())) {
                throw new ManifestException(
                        "Found reserved keyword " + alias.getKey() + " in interpolation context");
            }
            if (context.containsKey(alias.getValue())) {
                bindings.put(alias.getKey(), context.get(alias.getValue()));
            }
        }
        return bindings;
    }

    /** Python ast.literal_eval-style coercion of a rendered string. */
    static Object literalEval(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.strip();
        if (trimmed.isEmpty()) {
            return value;
        }
        if (INTEGER.matcher(trimmed).matches()) {
            String digits = trimmed.replace("_", "");
            String unsigned =
                    digits.startsWith("+") || digits.startsWith("-")
                            ? digits.substring(1)
                            : digits;
            // Python rejects leading zeros in integer literals ("03" stays a string).
            if (unsigned.length() > 1 && unsigned.startsWith("0")) {
                return value;
            }
            try {
                return Long.parseLong(digits);
            } catch (NumberFormatException e) {
                return new BigInteger(digits);
            }
        }
        if (FLOAT.matcher(trimmed).matches()) {
            return Double.parseDouble(trimmed.replace("_", ""));
        }
        switch (trimmed) {
            case "True":
                return Boolean.TRUE;
            case "False":
                return Boolean.FALSE;
            case "None":
                return null;
            default:
                // fall through to structural literals
        }
        char first = trimmed.charAt(0);
        if (first == '{' || first == '[' || first == '\'' || first == '"') {
            try {
                return LENIENT_JSON.readValue(
                        trimmed, new com.fasterxml.jackson.core.type.TypeReference<Object>() {});
            } catch (Exception ignored) {
                return value;
            }
        }
        return value;
    }
}
