package io.hevo.connector.neo.runtime;

import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.manifest.ManifestException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Request-level authentication, mirroring the CDK's declarative authenticators
 * (sources/declarative/auth/token.py). P1 scope: ApiKeyAuthenticator (header or query
 * parameter injection), BearerAuthenticator, BasicHttpAuthenticator, NoAuth. OAuth lands in P3.
 */
public final class Authenticator {

    private final Map<String, InterpolatedString> headers = new LinkedHashMap<>();
    private final Map<String, InterpolatedString> queryParams = new LinkedHashMap<>();
    private final Map<String, Object> config;
    private final boolean basic;
    private InterpolatedString basicUsername;
    private InterpolatedString basicPassword;

    private Authenticator(Map<String, Object> config, boolean basic) {
        this.config = config;
        this.basic = basic;
    }

    public static Authenticator from(Map<String, Object> component, Map<String, Object> config) {
        if (component == null) {
            return new Authenticator(config, false);
        }
        String type = Components.type(component);
        Map<String, Object> parameters = Components.parameters(component);
        switch (type == null ? "NoAuth" : type) {
            case "NoAuth" -> {
                return new Authenticator(config, false);
            }
            case "ApiKeyAuthenticator" -> {
                Authenticator auth = new Authenticator(config, false);
                String token = Components.getString(component, "api_token");
                RequestOptionSpec option =
                        RequestOptionSpec.from(Components.getMap(component, "inject_into"));
                InterpolatedString value = InterpolatedString.create(token, parameters);
                if (option == null) {
                    // CDK default header for ApiKeyAuthenticator
                    auth.headers.put("Authorization", value);
                    return auth;
                }
                String fieldName =
                        option.fieldName() == null
                                ? "Authorization"
                                : String.valueOf(option.fieldName().eval(config));
                switch (option.injectInto()) {
                    case HEADER -> auth.headers.put(fieldName, value);
                    case REQUEST_PARAMETER -> auth.queryParams.put(fieldName, value);
                    default ->
                            throw new ManifestException(
                                    "ApiKeyAuthenticator injection into "
                                            + option.injectInto()
                                            + " is not supported yet");
                }
                return auth;
            }
            case "BearerAuthenticator" -> {
                Authenticator auth = new Authenticator(config, false);
                String token = Components.getString(component, "api_token");
                auth.headers.put(
                        "Authorization",
                        InterpolatedString.create("Bearer " + token, parameters));
                return auth;
            }
            case "BasicHttpAuthenticator" -> {
                Authenticator auth = new Authenticator(config, true);
                String username = Components.getString(component, "username");
                String password = Components.getString(component, "password");
                auth.basicUsername =
                        InterpolatedString.create(username == null ? "" : username, parameters);
                auth.basicPassword =
                        InterpolatedString.create(password == null ? "" : password, parameters);
                return auth;
            }
            default ->
                    throw new ManifestException(
                            "Unsupported authenticator type for P1: " + type);
        }
    }

    public Map<String, String> authHeaders() {
        Map<String, String> evaluated = new LinkedHashMap<>();
        if (basic) {
            String credentials =
                    String.valueOf(basicUsername.eval(config))
                            + ":"
                            + String.valueOf(basicPassword.eval(config));
            evaluated.put(
                    "Authorization",
                    "Basic "
                            + Base64.getEncoder()
                                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
            return evaluated;
        }
        for (Map.Entry<String, InterpolatedString> header : headers.entrySet()) {
            evaluated.put(header.getKey(), String.valueOf(header.getValue().eval(config)));
        }
        return evaluated;
    }

    public Map<String, String> authQueryParams() {
        Map<String, String> evaluated = new LinkedHashMap<>();
        for (Map.Entry<String, InterpolatedString> param : queryParams.entrySet()) {
            evaluated.put(param.getKey(), String.valueOf(param.getValue().eval(config)));
        }
        return evaluated;
    }
}
