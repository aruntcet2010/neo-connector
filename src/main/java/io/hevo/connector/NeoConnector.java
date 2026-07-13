package io.hevo.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.catalog.core.namespace.Namespace;
import io.hevo.connector.cdk.saas.SaasConnector;
import io.hevo.connector.cdk.schema.ObjectSpec;
import io.hevo.connector.cdk.utils.LoadRuleUtils;
import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.hooks.PlatformHooks;
import io.hevo.connector.model.ConnectorContext;
import io.hevo.connector.model.ConnectorInitContext;
import io.hevo.connector.model.ExecutionResult;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectFailureInfo;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.PollTaskNode;
import io.hevo.connector.model.SourceObjectMapping;
import io.hevo.connector.model.v2.ObjectSchemaV2;
import io.hevo.connector.neo.manifest.ManifestPipeline;
import io.hevo.connector.neo.runtime.Components;
import io.hevo.connector.neo.runtime.DpathExtractor;
import io.hevo.connector.neo.runtime.ManifestPollTask;
import io.hevo.connector.neo.runtime.NeoRequester;
import io.hevo.connector.neo.runtime.Paginator;
import io.hevo.connector.neo.runtime.SchemaSynthesizer;
import io.hevo.connector.neo.runtime.StreamSpec;
import io.hevo.connector.processor.ConnectorProcessor;
import io.hevo.datapath.common.config.enums.CategoryType;
import io.hevo.datapath.common.load.LoadOperationRules;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic declarative connector: interprets a connector manifest (Airbyte low-code YAML) at runtime
 * and syncs the streams it defines.
 *
 * <p>Manifest resolution order: the {@code manifest_yaml} config property (inline, dev override),
 * then a manifest bundled on the classpath at {@code manifests/<manifest_ref>.yaml}, then {@code
 * manifests/<connectorId>.yaml} — so a connector registration generated from a manifest (see
 * tools/generate_registration.py) runs its bundled manifest with no manifest-related user input. A
 * manifest-store fetch keyed by {@code manifest_ref} will replace the bundled lookup.
 *
 * <p>The source settings the manifest's spec declares arrive as individual config properties named
 * after {@code spec.connection_specification.properties} (the generated registration renders them
 * as form fields), with the legacy {@code source_config_json} JSON object supported as an override.
 */
public class NeoConnector extends SaasConnector {

  private static final Logger log = LoggerFactory.getLogger(NeoConnector.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private ManifestPipeline.Manifest manifest;
  private Map<String, Object> sourceConfig = Map.of();
  private final Map<String, StreamSpec> streamSpecs = new LinkedHashMap<>();
  private final Map<String, ObjectSpec> objectSpecsByName = new LinkedHashMap<>();

  @Override
  public void init(ConnectorInitContext context) {
    if (context.hasProperty("source_config_json")) {
      try {
        this.sourceConfig =
            JSON.readValue(
                context.getString("source_config_json"),
                new TypeReference<Map<String, Object>>() {});
      } catch (Exception e) {
        throw new ConnectorException("source_config_json is not valid JSON", e);
      }
    }
    if (context.hasProperty("manifest_yaml")) {
      loadManifest(context.getString("manifest_yaml"));
    } else {
      String manifestYaml = null;
      if (context.hasProperty("manifest_ref")) {
        manifestYaml = readBundledManifest(context.getString("manifest_ref"));
      }
      if (manifestYaml == null) {
        manifestYaml = readBundledManifest(context.connectorId());
      }
      if (manifestYaml != null) {
        loadManifest(manifestYaml);
      }
    }
    if (manifest != null) {
      mergeSpecPropertiesIntoConfig(context);
    }
  }

  /**
   * Populate the Jinja {@code config} from the connector's own form fields: every property named in
   * the manifest's {@code spec.connection_specification.properties} that arrived in the init
   * context. Values from {@code source_config_json} win over individual fields.
   */
  @SuppressWarnings("unchecked")
  private void mergeSpecPropertiesIntoConfig(ConnectorInitContext context) {
    Map<String, Object> spec = manifest.spec();
    if (spec == null) {
      return;
    }
    Map<String, Object> connectionSpec = (Map<String, Object>) spec.get("connection_specification");
    if (connectionSpec == null) {
      return;
    }
    Map<String, Object> properties = (Map<String, Object>) connectionSpec.get("properties");
    if (properties == null || properties.isEmpty()) {
      return;
    }
    Map<String, Object> merged = new LinkedHashMap<>();
    for (String key : properties.keySet()) {
      if (context.hasProperty(key)) {
        merged.put(key, context.get(key));
      }
    }
    merged.putAll(sourceConfig);
    this.sourceConfig = merged;
  }

  /** Load a manifest bundled on the classpath under {@code manifests/<name>.yaml}, or null. */
  private static String readBundledManifest(String name) {
    if (name == null || name.isBlank()) {
      return null;
    }
    String resource = "/manifests/" + name + ".yaml";
    try (InputStream in = NeoConnector.class.getResourceAsStream(resource)) {
      return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      log.warn("Failed to read bundled manifest {}", resource, e);
      return null;
    }
  }

  private void loadManifest(String manifestYaml) {
    this.manifest = new ManifestPipeline().process(manifestYaml);
    streamSpecs.clear();
    objectSpecsByName.clear();
    for (Map<String, Object> stream : manifest.streams()) {
      StreamSpec spec = new StreamSpec(stream);
      streamSpecs.put(spec.name(), spec);
      objectSpecsByName.put(spec.name(), SchemaSynthesizer.synthesizeSpec(spec));
    }
    log.info("Loaded manifest version {} with {} streams", manifest.version(), streamSpecs.size());
  }

  @Override
  public List<String> getSchemaFiles() {
    // Schemas are synthesized from the manifest at runtime; there is no static schema.yml.
    return List.of();
  }

  @Override
  public void initializeConnection() throws ConnectorException {
    super.initializeConnection();
    if (manifest != null) {
      runCheckStream();
    }
  }

  /** The manifest's CheckStream: read the first page of the declared check stream. */
  private void runCheckStream() throws ConnectorException {
    Map<String, Object> check = manifest.check();
    List<Object> streamNames = check == null ? null : Components.getList(check, "stream_names");
    if (streamNames == null || streamNames.isEmpty()) {
      return;
    }
    String streamName = String.valueOf(streamNames.get(0));
    StreamSpec stream = streamSpecs.get(streamName);
    if (stream == null) {
      throw new ConnectorException("Check stream not found in manifest: " + streamName);
    }
    try {
      NeoRequester requester = new NeoRequester(stream.requester(), sourceConfig);
      Paginator paginator = Paginator.from(stream.paginator(), sourceConfig);
      var response =
          saasHttpClient.execute(
              requester.buildRequest(stream.name(), paginator.initialToken(), paginator));
      response.throwForStatus();
      new DpathExtractor(stream.extractor(), sourceConfig).extractRecords(response.body());
      log.info("Check stream {} succeeded", streamName);
    } catch (ConnectorException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectorException("Connection check failed on stream " + streamName, e);
    }
  }

  /**
   * SDP platform: list objects from the manifest's streams (dynamic counterpart of {@link
   * SaasConnector#fetchObjects()} serving from parsed schema.yml specs).
   */
  @Override
  public List<io.hevo.connector.model.v2.ObjectDetails> fetchObjects() throws ConnectorException {
    return objectSpecsByName.values().stream()
        .map(spec -> new io.hevo.connector.model.v2.ObjectDetails(spec.namespace(), spec.status()))
        .toList();
  }

  /** SDP platform: V2 schemas synthesized from the manifest streams' JSON schemas. */
  @Override
  public List<ObjectSchemaV2> fetchSourceSchema(List<Namespace> objects) throws ConnectorException {
    Set<Namespace> selected = new HashSet<>(objects);
    return objectSpecsByName.values().stream()
        .filter(spec -> selected.contains(spec.namespace()))
        .map(ObjectSpec::getSchema)
        .toList();
  }

  /** SDP platform: load rules derived from manifest sync semantics (PK → merge, etc.). */
  @Override
  public LoadOperationRules fetchLoadOperationRules(
      Namespace sourceObject, SourceObjectMapping sourceObjectMapping, CategoryType categoryType) {
    ObjectSpec spec = objectSpecsByName.get(sourceObject.k0());
    if (spec == null || spec.loadStrategy() == null) {
      log.error("load operation rules were not found for object {}", sourceObject);
      throw new ConnectorException(
          "load operation rules were not found for object " + sourceObject);
    }
    return LoadRuleUtils.convertToLoadOperationRules(
        categoryType, spec.loadStrategy(), sourceObjectMapping);
  }

  @Override
  public List<ObjectDetails> getObjects() throws ConnectorException {
    List<ObjectDetails> objects = new ArrayList<>();
    for (StreamSpec stream : streamSpecs.values()) {
      objects.add(SchemaSynthesizer.synthesize(stream).objectDetail());
    }
    return objects;
  }

  @Override
  public List<ObjectSchema> fetchSchemaFromSource(List<ObjectDetails> objects)
      throws ConnectorException {
    List<ObjectSchema> schemas = new ArrayList<>();
    for (ObjectDetails object : objects) {
      StreamSpec stream = streamSpecs.get(object.getTableFullyQualifiedName());
      if (stream != null) {
        schemas.add(SchemaSynthesizer.synthesize(stream));
      }
    }
    return schemas;
  }

  @Override
  public ExecutionResult fetchDataFromSource(ConnectorContext context, ConnectorProcessor processor)
      throws ConnectorException {
    throw new ConnectorException(
        "The fetchDataFromSource pattern is deprecated for this connector; it uses the"
            + " CDK PollTask pattern via generateTasks().");
  }

  @Override
  public List<PollTaskNode> generateTasks(
      CategoryType categoryType, PlatformHooks platformHooks, Set<Namespace> activeObjects) {
    List<PollTaskNode> tasks = new ArrayList<>();
    for (Namespace object : activeObjects) {
      StreamSpec stream = streamSpecs.get(object.k0());
      if (stream == null) {
        log.error("[ALERT] No manifest stream for object: {}", object.k0());
        platformHooks.markSourceObjectAsFailed(
            object,
            new ObjectFailureInfo.Builder()
                .setReason("No manifest stream named: " + object.k0())
                .setPermanent(true)
                .build());
        continue;
      }
      tasks.add(
          new PollTaskNode(
              stream.name(),
              new ManifestPollTask(
                  platformHooks, categoryType, stream, saasHttpClient, sourceConfig),
              List.of()));
    }
    return tasks;
  }

  ManifestPipeline.Manifest manifest() {
    return manifest;
  }
}
