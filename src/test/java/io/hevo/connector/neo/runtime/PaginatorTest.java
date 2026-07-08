package io.hevo.connector.neo.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.hevo.connector.neo.manifest.ManifestLoader;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PaginatorTest {

    private static Paginator paginator(String yaml) {
        return Paginator.from(ManifestLoader.load(yaml), Map.of());
    }

    private static Paginator.PageContext page(int lastPageSize, Object lastToken) {
        return new Paginator.PageContext(Map.of(), Map.of(), lastPageSize, Map.of(), lastToken);
    }

    @Test
    void offsetIncrementAccumulatesAndStops() {
        Paginator p =
                paginator(
                        """
                        type: DefaultPaginator
                        page_token_option:
                          type: RequestOption
                          inject_into: request_parameter
                          field_name: offset
                        pagination_strategy:
                          type: OffsetIncrement
                          page_size: 2
                        """);
        assertNull(p.initialToken());
        // Full page of 2 → next offset = 0 + 2
        assertEquals(2, p.nextPageToken(page(2, null)));
        // Next full page → 2 + 2
        assertEquals(4, p.nextPageToken(page(2, 2)));
        // Short page → stop
        assertNull(p.nextPageToken(page(1, 4)));
        // Empty page → stop
        assertNull(p.nextPageToken(page(0, 4)));
        // Token injection
        assertEquals(
                Map.of("offset", 2),
                p.requestOptions(RequestOptionSpec.InjectInto.REQUEST_PARAMETER, 2));
    }

    @Test
    void pageIncrementCountsPages() {
        Paginator p =
                paginator(
                        """
                        type: DefaultPaginator
                        page_token_option:
                          type: RequestOption
                          inject_into: request_parameter
                          field_name: page
                        page_size_option:
                          type: RequestOption
                          inject_into: request_parameter
                          field_name: per_page
                        pagination_strategy:
                          type: PageIncrement
                          page_size: 3
                          start_from_page: 1
                          inject_on_first_request: true
                        """);
        assertEquals(1, p.initialToken());
        assertEquals(2, p.nextPageToken(page(3, 1)));
        assertNull(p.nextPageToken(page(2, 2)));
        // Both token and page size injected
        assertEquals(
                Map.of("page", 2, "per_page", 3),
                p.requestOptions(RequestOptionSpec.InjectInto.REQUEST_PARAMETER, 2));
    }

    @Test
    void cursorPaginationReadsResponseAndStops() {
        Paginator p =
                paginator(
                        """
                        type: DefaultPaginator
                        page_token_option:
                          type: RequestOption
                          inject_into: request_parameter
                          field_name: cursor
                        pagination_strategy:
                          type: CursorPagination
                          cursor_value: "{{ response.next_cursor }}"
                        """);
        assertNull(p.initialToken());
        Paginator.PageContext withCursor =
                new Paginator.PageContext(
                        Map.of("next_cursor", "abc123"), Map.of(), 5, Map.of(), null);
        assertEquals("abc123", p.nextPageToken(withCursor));
        // No cursor in the response → template unresolved → stop
        Paginator.PageContext withoutCursor =
                new Paginator.PageContext(Map.of("data", List.of()), Map.of(), 5, Map.of(), "abc");
        assertNull(p.nextPageToken(withoutCursor));
    }

    @Test
    void cursorPaginationStopCondition() {
        Paginator p =
                paginator(
                        """
                        type: DefaultPaginator
                        page_token_option:
                          type: RequestOption
                          inject_into: request_parameter
                          field_name: cursor
                        pagination_strategy:
                          type: CursorPagination
                          cursor_value: "{{ response.next }}"
                          stop_condition: "{{ response.next is none }}"
                        """);
        Paginator.PageContext hasNext =
                new Paginator.PageContext(Map.of("next", "n2"), Map.of(), 2, Map.of(), null);
        assertEquals("n2", p.nextPageToken(hasNext));
        Paginator.PageContext noNext =
                new Paginator.PageContext(Map.of("done", true), Map.of(), 2, Map.of(), "n2");
        assertNull(p.nextPageToken(noNext));
    }

    @Test
    void noPaginationIsSinglePage() {
        Paginator p = paginator("type: NoPagination");
        assertNull(p.initialToken());
        assertNull(p.nextPageToken(page(50, null)));
    }
}
