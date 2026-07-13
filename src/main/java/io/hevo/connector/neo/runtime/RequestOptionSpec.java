package io.hevo.connector.neo.runtime;

import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.Map;

/**
 * A manifest RequestOption: where to inject a value (query parameter, header, body) and under which
 * field name. The field name is itself interpolatable.
 */
public record RequestOptionSpec(InjectInto injectInto, InterpolatedString fieldName) {

  public enum InjectInto {
    REQUEST_PARAMETER,
    HEADER,
    BODY_DATA,
    BODY_JSON,
    PATH
  }

  public static RequestOptionSpec from(Map<String, Object> component) {
    if (component == null) {
      return null;
    }
    String type = Components.type(component);
    if ("RequestPath".equals(type)) {
      return new RequestOptionSpec(InjectInto.PATH, null);
    }
    String injectInto = Components.getString(component, "inject_into");
    if (injectInto == null) {
      throw new ManifestException("RequestOption missing inject_into: " + component);
    }
    InjectInto target =
        switch (injectInto) {
          case "request_parameter" -> InjectInto.REQUEST_PARAMETER;
          case "header" -> InjectInto.HEADER;
          case "body_data" -> InjectInto.BODY_DATA;
          case "body_json" -> InjectInto.BODY_JSON;
          case "path" -> InjectInto.PATH;
          default -> throw new ManifestException(
              "Unsupported RequestOption inject_into: " + injectInto);
        };
    String fieldName = Components.getString(component, "field_name");
    return new RequestOptionSpec(
        target,
        fieldName == null
            ? null
            : InterpolatedString.create(fieldName, Components.parameters(component)));
  }
}
