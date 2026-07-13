package io.hevo.connector.neo.interpolation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A manifest string evaluated with Jinja at read time. Mirrors the CDK's InterpolatedString,
 * including the plain-string fast path and the default-falls-back-to-the-template rule.
 */
public final class InterpolatedString {

  private static final JinjaEngine ENGINE = new JinjaEngine();

  private final String string;
  private final String defaultTemplate;
  private final Map<String, Object> parameters;
  private Boolean isPlainString;

  private InterpolatedString(
      String string, String defaultTemplate, Map<String, Object> parameters) {
    this.string = string;
    this.defaultTemplate = defaultTemplate != null ? defaultTemplate : string;
    this.parameters = parameters == null ? Map.of() : parameters;
  }

  public static InterpolatedString create(String string, Map<String, Object> parameters) {
    return new InterpolatedString(string, null, parameters);
  }

  public static InterpolatedString create(
      String string, String defaultTemplate, Map<String, Object> parameters) {
    return new InterpolatedString(string, defaultTemplate, parameters);
  }

  public Object eval(Map<String, Object> config) {
    return eval(config, Map.of());
  }

  public Object eval(Map<String, Object> config, Map<String, Object> additionalContext) {
    if (Boolean.TRUE.equals(isPlainString)) {
      return string;
    }
    Map<String, Object> context = new LinkedHashMap<>();
    context.put("config", config == null ? Map.of() : config);
    context.put("parameters", parameters);
    context.putAll(additionalContext);
    Object evaluated = ENGINE.eval(string, context, defaultTemplate);
    if (isPlainString == null) {
      isPlainString = string.equals(evaluated);
    }
    return evaluated;
  }

  public String string() {
    return string;
  }
}
