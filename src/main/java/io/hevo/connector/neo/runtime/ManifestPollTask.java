package io.hevo.connector.neo.runtime;

import io.hevo.connector.cdk.model.PollResult;
import io.hevo.connector.cdk.offset.SimpleOffset;
import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.cdk.saas.task.SaasObjectPollTask;
import io.hevo.connector.hooks.PlatformHooks;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Hevo poll-task adapter for one manifest stream: delegates the entire read to
 * {@link StreamReader} (the same engine the test-read CLI runs), normalizes records for the
 * publisher's strict casts, and round-trips the cursor state through the {@link SimpleOffset}
 * under {@link #STATE_KEY}.
 */
public class ManifestPollTask extends SaasObjectPollTask {

  private static final Logger log = LoggerFactory.getLogger(ManifestPollTask.class);
  static final String STATE_KEY = "state";

  private final StreamSpec stream;
  private final StreamReader streamReader;
  private final RecordNormalizer recordNormalizer;

  public ManifestPollTask(
      PlatformHooks platformHooks,
      CategoryType categoryType,
      StreamSpec stream,
      SaasHttpClient httpClient,
      Map<String, Object> config) {
    super(platformHooks, categoryType, stream.namespace());
    this.stream = stream;
    this.streamReader = new StreamReader(stream, httpClient, config);
    this.recordNormalizer = new RecordNormalizer(stream);
  }

  @Override
  public PollResult executePoll(SimpleOffset offset, CategoryType categoryType) {
    String stateJson = offset == null || offset.isEmpty() ? null : offset.get(STATE_KEY);
    ReadResult result =
        read(
            record -> publishRecord(stream.namespace(), recordNormalizer.normalize(record)),
            stateJson);
    log.info("Stream {}: published {} records", stream.name(), result.recordCount());
    SimpleOffset nextOffset = SimpleOffset.empty();
    if (result.stateJson() != null) {
      nextOffset.set(STATE_KEY, result.stateJson());
    }
    return PollResult.success(nextOffset);
  }

  public record ReadResult(long recordCount, String stateJson) {}

  /** Test-friendly entry: read everything with no prior state. */
  public long readStream(Consumer<Object> recordConsumer) {
    return read(r -> recordConsumer.accept(r), null).recordCount();
  }

  public ReadResult read(Consumer<Map<String, Object>> recordConsumer, String stateJson) {
    StreamReader.ReadResult result = streamReader.read(recordConsumer, stateJson);
    return new ReadResult(result.recordCount(), result.stateJson());
  }
}
