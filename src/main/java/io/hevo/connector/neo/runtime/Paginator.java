package io.hevo.connector.neo.runtime;

import io.hevo.connector.neo.interpolation.InterpolatedBoolean;
import io.hevo.connector.neo.interpolation.InterpolatedString;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DefaultPaginator + pagination strategies, mirroring
 * requesters/paginators/{default_paginator.py, strategies/*.py}:
 *
 * <ul>
 *   <li>PageIncrement: stop when {@code last_page_size < page_size} or 0 records; token counts
 *       pages from {@code start_from_page}.
 *   <li>OffsetIncrement: stop under the same rule; token accumulates {@code last_page_size}.
 *   <li>CursorPagination: token = interpolated {@code cursor_value} with {@code response},
 *       {@code headers}, {@code last_record}, {@code last_page_size} in context; stops when the
 *       {@code stop_condition} is truthy or the cursor renders empty.
 * </ul>
 *
 * The paginator injects the token via {@code page_token_option} (query param / header / path) and
 * the page size via {@code page_size_option}.
 */
public final class Paginator {

    /** Everything a strategy may consult when computing the next token. */
    public record PageContext(
            Object decodedResponse,
            Map<String, Object> headers,
            int lastPageSize,
            Object lastRecord,
            Object lastPageTokenValue) {}

    public interface Strategy {
        /** Token for the first request, or null to send none. */
        Object initialToken();

        /** Next token, or null to stop paginating. */
        Object nextPageToken(PageContext context);

        default Integer pageSize() {
            return null;
        }
    }

    private final Strategy strategy;
    private final RequestOptionSpec pageTokenOption;
    private final RequestOptionSpec pageSizeOption;
    private final Map<String, Object> config;

    private Paginator(
            Strategy strategy,
            RequestOptionSpec pageTokenOption,
            RequestOptionSpec pageSizeOption,
            Map<String, Object> config) {
        this.strategy = strategy;
        this.pageTokenOption = pageTokenOption;
        this.pageSizeOption = pageSizeOption;
        this.config = config;
    }

    public static Paginator from(Map<String, Object> component, Map<String, Object> config) {
        if (component == null || "NoPagination".equals(Components.type(component))) {
            return new Paginator(new NoPagination(), null, null, config);
        }
        String type = Components.type(component);
        if (!"DefaultPaginator".equals(type)) {
            throw new ManifestException("Unsupported paginator type: " + type);
        }
        Map<String, Object> strategyComponent = Components.getMap(component, "pagination_strategy");
        Strategy strategy = strategyFrom(strategyComponent, config);
        return new Paginator(
                strategy,
                RequestOptionSpec.from(Components.getMap(component, "page_token_option")),
                RequestOptionSpec.from(Components.getMap(component, "page_size_option")),
                config);
    }

    private static Strategy strategyFrom(
            Map<String, Object> component, Map<String, Object> config) {
        String type = Components.type(component);
        if (type == null) {
            throw new ManifestException("Paginator strategy missing type: " + component);
        }
        return switch (type) {
            case "PageIncrement" -> new PageIncrement(component, config);
            case "OffsetIncrement" -> new OffsetIncrement(component, config);
            case "CursorPagination" -> new CursorPagination(component, config);
            default -> throw new ManifestException("Unsupported pagination strategy: " + type);
        };
    }

    public Object initialToken() {
        return strategy.initialToken();
    }

    public Object nextPageToken(PageContext context) {
        return strategy.nextPageToken(context);
    }

    /** Query params / headers the paginator contributes for the given token. */
    public Map<String, Object> requestOptions(
            RequestOptionSpec.InjectInto target, Object token) {
        Map<String, Object> options = new LinkedHashMap<>();
        if (pageTokenOption != null
                && pageTokenOption.injectInto() == target
                && token != null
                && pageTokenOption.fieldName() != null) {
            options.put(String.valueOf(pageTokenOption.fieldName().eval(config)), token);
        }
        if (pageSizeOption != null
                && pageSizeOption.injectInto() == target
                && strategy.pageSize() != null
                && pageSizeOption.fieldName() != null) {
            options.put(
                    String.valueOf(pageSizeOption.fieldName().eval(config)),
                    strategy.pageSize());
        }
        return options;
    }

    /** When the token option is a RequestPath, the token replaces the request path entirely. */
    public boolean tokenIsRequestPath() {
        return pageTokenOption != null
                && pageTokenOption.injectInto() == RequestOptionSpec.InjectInto.PATH;
    }

    static final class NoPagination implements Strategy {
        @Override
        public Object initialToken() {
            return null;
        }

        @Override
        public Object nextPageToken(PageContext context) {
            return null;
        }
    }

    static final class PageIncrement implements Strategy {
        private final Integer pageSize;
        private final int startFromPage;
        private final boolean injectOnFirstRequest;

        PageIncrement(Map<String, Object> component, Map<String, Object> config) {
            Object rawPageSize = component.get("page_size");
            this.pageSize = evalPageSize(rawPageSize, config);
            String start = Components.getString(component, "start_from_page");
            this.startFromPage = start == null ? 0 : Integer.parseInt(start);
            this.injectOnFirstRequest =
                    Boolean.TRUE.equals(component.get("inject_on_first_request"));
        }

        @Override
        public Object initialToken() {
            return injectOnFirstRequest ? startFromPage : null;
        }

        @Override
        public Object nextPageToken(PageContext context) {
            if ((pageSize != null && context.lastPageSize() < pageSize)
                    || context.lastPageSize() == 0) {
                return null;
            }
            Object last = context.lastPageTokenValue();
            if (last == null) {
                return startFromPage + 1;
            }
            if (!(last instanceof Number n)) {
                throw new ManifestException("Page token is not an integer: " + last);
            }
            return n.intValue() + 1;
        }

        @Override
        public Integer pageSize() {
            return pageSize;
        }
    }

    static final class OffsetIncrement implements Strategy {
        private final InterpolatedString pageSizeTemplate;
        private final boolean injectOnFirstRequest;
        private final Map<String, Object> config;

        OffsetIncrement(Map<String, Object> component, Map<String, Object> config) {
            this.config = config;
            Object rawPageSize = component.get("page_size");
            this.pageSizeTemplate =
                    rawPageSize == null
                            ? null
                            : InterpolatedString.create(
                                    String.valueOf(rawPageSize), Components.parameters(component));
            this.injectOnFirstRequest =
                    Boolean.TRUE.equals(component.get("inject_on_first_request"));
        }

        @Override
        public Object initialToken() {
            return injectOnFirstRequest ? 0 : null;
        }

        @Override
        public Object nextPageToken(PageContext context) {
            Integer pageSize = evaluatedPageSize(context);
            if ((pageSize != null && context.lastPageSize() < pageSize)
                    || context.lastPageSize() == 0) {
                return null;
            }
            Object last = context.lastPageTokenValue();
            if (last == null) {
                return context.lastPageSize();
            }
            if (!(last instanceof Number n)) {
                throw new ManifestException("Offset token is not an integer: " + last);
            }
            return n.intValue() + context.lastPageSize();
        }

        @Override
        public Integer pageSize() {
            return pageSizeTemplate == null
                    ? null
                    : evalPageSize(pageSizeTemplate.eval(config), config);
        }

        private Integer evaluatedPageSize(PageContext context) {
            if (pageSizeTemplate == null) {
                return null;
            }
            Object evaluated =
                    pageSizeTemplate.eval(
                            config, Map.of("response", contextResponse(context)));
            return evalPageSize(evaluated, config);
        }

        private static Object contextResponse(PageContext context) {
            return context.decodedResponse() == null ? Map.of() : context.decodedResponse();
        }
    }

    static final class CursorPagination implements Strategy {
        private final InterpolatedString cursorValue;
        private final InterpolatedBoolean stopCondition;
        private final Integer pageSize;
        private final Map<String, Object> config;

        CursorPagination(Map<String, Object> component, Map<String, Object> config) {
            this.config = config;
            String cursor = Components.getString(component, "cursor_value");
            if (cursor == null) {
                throw new ManifestException("CursorPagination requires cursor_value");
            }
            Map<String, Object> parameters = Components.parameters(component);
            this.cursorValue = InterpolatedString.create(cursor, parameters);
            String stop = Components.getString(component, "stop_condition");
            this.stopCondition = stop == null ? null : InterpolatedBoolean.create(stop, parameters);
            this.pageSize = evalPageSize(component.get("page_size"), config);
        }

        @Override
        public Object initialToken() {
            return null;
        }

        @Override
        public Object nextPageToken(PageContext context) {
            Map<String, Object> evalContext = new LinkedHashMap<>();
            evalContext.put(
                    "response",
                    context.decodedResponse() == null ? Map.of() : context.decodedResponse());
            evalContext.put("headers", context.headers());
            evalContext.put(
                    "last_record", context.lastRecord() == null ? Map.of() : context.lastRecord());
            evalContext.put("last_page_size", context.lastPageSize());
            if (stopCondition != null && stopCondition.eval(config, evalContext)) {
                return null;
            }
            Object token = cursorValue.eval(config, evalContext);
            // Python: `token if token else None` — falsy tokens end pagination.
            if (token == null
                    || "".equals(token)
                    || Boolean.FALSE.equals(token)
                    || (token instanceof Number n && n.doubleValue() == 0.0)) {
                return null;
            }
            // An unresolved template (eval fell back to the raw string) means no cursor in the
            // response.
            if (token instanceof String s && s.contains("{{")) {
                return null;
            }
            return token;
        }

        @Override
        public Integer pageSize() {
            return pageSize;
        }
    }

    private static Integer evalPageSize(Object value, Map<String, Object> config) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        String s = String.valueOf(value);
        if (s.contains("{{")) {
            Object evaluated =
                    InterpolatedString.create(s, Map.of()).eval(config, Map.of("response", Map.of()));
            if (evaluated instanceof Number n) {
                return n.intValue();
            }
            return Integer.parseInt(String.valueOf(evaluated));
        }
        return Integer.parseInt(s.trim());
    }

    /** Flattens multi-valued HTTP headers into single values, keeping lists only when needed. */
    public static Map<String, Object> flattenHeaders(Map<String, List<String>> headers) {
        Map<String, Object> flattened = new LinkedHashMap<>();
        if (headers != null) {
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                List<String> values = entry.getValue();
                flattened.put(
                        entry.getKey().toLowerCase(),
                        values == null || values.isEmpty()
                                ? ""
                                : values.size() == 1 ? values.get(0) : values);
            }
        }
        return flattened;
    }
}
