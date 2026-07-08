package io.hevo.connector.neo.interpolation;

import java.util.List;
import java.util.Map;

/**
 * A manifest boolean condition template, mirroring the CDK's InterpolatedBoolean: the evaluated
 * result is truthy unless it appears in the CDK's FALSE_VALUES list (interpolated_boolean.py):
 * the strings "False", "false", "{}", "[]", "()", "", "0", "0.0", plus false, empty collections,
 * and zero numbers. Defaults to false when the condition is absent or renders empty.
 */
public final class InterpolatedBoolean {

    private static final JinjaEngine ENGINE = new JinjaEngine();
    private static final List<Object> FALSE_VALUES =
            List.of("False", "false", "{}", "[]", "()", "", "0", "0.0");

    private final String condition;
    private final Map<String, Object> parameters;

    private InterpolatedBoolean(String condition, Map<String, Object> parameters) {
        this.condition = condition;
        this.parameters = parameters == null ? Map.of() : parameters;
    }

    public static InterpolatedBoolean create(String condition, Map<String, Object> parameters) {
        return new InterpolatedBoolean(condition, parameters);
    }

    public boolean eval(Map<String, Object> config, Map<String, Object> additionalContext) {
        if (condition == null) {
            return false;
        }
        Map<String, Object> context = new java.util.LinkedHashMap<>();
        context.put("config", config == null ? Map.of() : config);
        context.put("parameters", parameters);
        context.putAll(additionalContext);
        Object result = ENGINE.eval(condition, context, "False");
        return isTruthy(result);
    }

    private static boolean isTruthy(Object value) {
        if (value == null || Boolean.FALSE.equals(value)) {
            return false;
        }
        if (value instanceof String s) {
            return !FALSE_VALUES.contains(s);
        }
        if (value instanceof Number n) {
            return n.doubleValue() != 0.0;
        }
        if (value instanceof Map<?, ?> m) {
            return !m.isEmpty();
        }
        if (value instanceof List<?> l) {
            return !l.isEmpty();
        }
        return true;
    }
}
