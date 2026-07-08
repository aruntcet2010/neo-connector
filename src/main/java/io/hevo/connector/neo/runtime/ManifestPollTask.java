package io.hevo.connector.neo.runtime;

import io.hevo.connector.cdk.model.PollResult;
import io.hevo.connector.cdk.offset.SimpleOffset;
import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.cdk.saas.http.SaasHttpRequest;
import io.hevo.connector.cdk.saas.http.SaasHttpResponse;
import io.hevo.connector.cdk.saas.task.SaasObjectPollTask;
import io.hevo.connector.hooks.PlatformHooks;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The generic poll task interpreting one manifest stream. The read loop mirrors the CDK's
 * SimpleRetriever._read_pages: initial token → fetch page → extract records → compute next token
 * from (response, last_page_size, last_record, last_token) → stop when the token is null.
 *
 * <p>P1 scope: full refresh, single partition, no cursor — every poll rereads the stream and
 * returns an empty offset.
 */
public class ManifestPollTask extends SaasObjectPollTask {

    private static final Logger log = LoggerFactory.getLogger(ManifestPollTask.class);
    /** Backstop against paginators that never terminate (misconfigured cursors). */
    private static final int MAX_PAGES = 10_000;

    private final StreamSpec stream;
    private final SaasHttpClient httpClient;
    private final NeoRequester requester;
    private final Paginator paginator;
    private final DpathExtractor extractor;

    public ManifestPollTask(
            PlatformHooks platformHooks,
            CategoryType categoryType,
            StreamSpec stream,
            SaasHttpClient httpClient,
            Map<String, Object> config) {
        super(platformHooks, categoryType, stream.namespace());
        this.stream = stream;
        this.httpClient = httpClient;
        this.requester = new NeoRequester(stream.requester(), config);
        this.paginator = Paginator.from(stream.paginator(), config);
        this.extractor = new DpathExtractor(stream.extractor(), config);
    }

    @Override
    public PollResult executePoll(SimpleOffset offset, CategoryType categoryType) {
        long published = readStream(record -> publishRecord(stream.namespace(), asMap(record)));
        log.info("Stream {}: published {} records", stream.name(), published);
        return PollResult.success(SimpleOffset.empty());
    }

    /** Reads all pages, invoking the consumer per record; returns the record count. */
    public long readStream(java.util.function.Consumer<Object> recordConsumer) {
        long total = 0;
        Object token = paginator.initialToken();
        for (int page = 0; page < MAX_PAGES; page++) {
            SaasHttpRequest request = requester.buildRequest(stream.name(), token, paginator);
            SaasHttpResponse<String> response = httpClient.execute(request);
            response.throwForStatus();

            List<Object> records = extractor.extractRecords(response.body());
            Object lastRecord = records.isEmpty() ? null : records.get(records.size() - 1);
            for (Object record : records) {
                recordConsumer.accept(record);
            }
            total += records.size();

            Paginator.PageContext context =
                    new Paginator.PageContext(
                            DpathExtractor.decode(response.body()),
                            Paginator.flattenHeaders(response.headers()),
                            records.size(),
                            lastRecord,
                            token);
            token = paginator.nextPageToken(context);
            if (token == null) {
                return total;
            }
        }
        log.warn("Stream {}: pagination did not terminate after {} pages", stream.name(), MAX_PAGES);
        return total;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object record) {
        if (record instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        Map<String, Object> wrapped = new LinkedHashMap<>();
        wrapped.put("value", record);
        return wrapped;
    }
}
