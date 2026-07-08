package io.hevo.connector.neo.runtime;

import io.hevo.connector.cdk.saas.http.SaasHttpRequest;
import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builds a {@link SaasHttpRequest} for one page of one stream from an HttpRequester component,
 * mirroring the CDK's request assembly:
 *
 * <ul>
 *   <li>URL = interpolated {@code url_base} + {@code path} (or the paginator token when the
 *       page_token_option is a RequestPath).
 *   <li>Query params / headers merge requester options, authenticator options, and paginator
 *       options — duplicate keys RAISE (utils/mapping_helpers.py), they never silently overwrite.
 *   <li>Templates see {@code config}, {@code stream_slice} (empty in P1), and
 *       {@code next_page_token} = {"next_page_token": token}.
 * </ul>
 */
public final class NeoRequester {

    private final Map<String, Object> requester;
    private final Map<String, Object> config;
    private final Authenticator authenticator;
    private final InterpolatedString urlBase;
    private final InterpolatedString path;
    private final String httpMethod;
    private final Map<String, Object> parameters;

    public NeoRequester(Map<String, Object> requester, Map<String, Object> config) {
        this.requester = requester;
        this.config = config;
        this.parameters = Components.parameters(requester);
        this.authenticator =
                Authenticator.from(Components.getMap(requester, "authenticator"), config);
        String base = Components.getString(requester, "url_base");
        if (base == null) {
            throw new ManifestException("HttpRequester missing url_base");
        }
        this.urlBase = InterpolatedString.create(base, parameters);
        String rawPath = Components.getString(requester, "path");
        this.path = rawPath == null ? null : InterpolatedString.create(rawPath, parameters);
        String method = Components.getString(requester, "http_method");
        this.httpMethod = method == null ? "GET" : method.toUpperCase();
    }

    public SaasHttpRequest buildRequest(
            String streamName, Object pageToken, Paginator paginator) {
        Map<String, Object> context = interpolationContext(pageToken);

        String url = joinUrl(evalToString(urlBase, context), resolvePath(context, pageToken, paginator));

        SaasHttpRequest.Builder builder =
                SaasHttpRequest.builder().method(httpMethod).url(url).objectName(streamName);

        // Query parameters: requester's own + authenticator + paginator. Collisions raise.
        Map<String, Object> params =
                mergeOptions(
                        "query parameter",
                        evaluateMapOption("request_parameters", context),
                        new LinkedHashMap<>(authenticator.authQueryParams()),
                        paginator.requestOptions(
                                RequestOptionSpec.InjectInto.REQUEST_PARAMETER, pageToken));
        params.forEach((k, v) -> builder.queryParam(k, stringValue(v)));

        // Headers: same merge discipline.
        Map<String, Object> headers =
                mergeOptions(
                        "header",
                        evaluateMapOption("request_headers", context),
                        new LinkedHashMap<>(authenticator.authHeaders()),
                        paginator.requestOptions(RequestOptionSpec.InjectInto.HEADER, pageToken));
        headers.forEach((k, v) -> builder.header(k, stringValue(v)));

        // Bodies (P1: static maps with interpolated values).
        Map<String, Object> bodyJson =
                mergeOptions(
                        "body_json",
                        evaluateMapOption("request_body_json", context),
                        Map.of(),
                        paginator.requestOptions(
                                RequestOptionSpec.InjectInto.BODY_JSON, pageToken));
        Map<String, Object> bodyData =
                mergeOptions(
                        "body_data",
                        evaluateMapOption("request_body_data", context),
                        Map.of(),
                        paginator.requestOptions(
                                RequestOptionSpec.InjectInto.BODY_DATA, pageToken));
        if (!bodyJson.isEmpty() && !bodyData.isEmpty()) {
            throw new ManifestException(
                    "HttpRequester declares both request_body_json and request_body_data");
        }
        if (!bodyJson.isEmpty()) {
            builder.jsonBody(bodyJson);
        } else if (!bodyData.isEmpty()) {
            Map<String, String> form = new LinkedHashMap<>();
            bodyData.forEach((k, v) -> form.put(k, stringValue(v)));
            builder.formBody(form);
        }

        return builder.build();
    }

    private Map<String, Object> interpolationContext(Object pageToken) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("stream_slice", Map.of());
        context.put(
                "next_page_token",
                pageToken == null ? Map.of() : Map.of("next_page_token", pageToken));
        return context;
    }

    private String resolvePath(
            Map<String, Object> context, Object pageToken, Paginator paginator) {
        if (paginator.tokenIsRequestPath() && pageToken != null) {
            return String.valueOf(pageToken);
        }
        return path == null ? "" : evalToString(path, context);
    }

    /** URL join matching the CDK: absolute paths (http...) win; else base/path with one slash. */
    private static String joinUrl(String base, String path) {
        if (path.startsWith("http://") || path.startsWith("https://")) {
            return path;
        }
        if (path.isEmpty()) {
            return base;
        }
        return base.replaceAll("/+$", "") + "/" + path.replaceAll("^/+", "");
    }

    private Map<String, Object> evaluateMapOption(String key, Map<String, Object> context) {
        Map<String, Object> raw = Components.getMap(requester, key);
        Map<String, Object> evaluated = new LinkedHashMap<>();
        if (raw == null) {
            return evaluated;
        }
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            String name =
                    String.valueOf(
                            InterpolatedString.create(entry.getKey(), parameters)
                                    .eval(config, context));
            Object value = entry.getValue();
            if (value instanceof String s) {
                value = InterpolatedString.create(s, parameters).eval(config, context);
            }
            evaluated.put(name, value);
        }
        return evaluated;
    }

    /** combine_mappings semantics: duplicate keys with different values raise. */
    @SafeVarargs
    private static Map<String, Object> mergeOptions(
            String target, Map<String, Object>... sources) {
        Map<String, Object> merged = new LinkedHashMap<>();
        for (Map<String, Object> source : sources) {
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                if (merged.containsKey(entry.getKey())
                        && !Objects.equals(merged.get(entry.getKey()), entry.getValue())) {
                    throw new ManifestException(
                            "Request "
                                    + target
                                    + " collision: duplicate key '"
                                    + entry.getKey()
                                    + "' with different values");
                }
                merged.put(entry.getKey(), entry.getValue());
            }
        }
        return merged;
    }

    private Object evalToObject(InterpolatedString template, Map<String, Object> context) {
        return template.eval(config, context);
    }

    private String evalToString(InterpolatedString template, Map<String, Object> context) {
        return String.valueOf(evalToObject(template, context));
    }

    private static String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }
}
