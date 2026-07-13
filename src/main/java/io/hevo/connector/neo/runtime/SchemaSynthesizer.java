package io.hevo.connector.neo.runtime;

import io.hevo.catalog.core.field.schema.base.Field;
import io.hevo.catalog.core.field.schema.enumeration.FieldState;
import io.hevo.catalog.core.field.schema.hudt.HField;
import io.hevo.catalog.model.enumeration.SourceObjectStatus;
import io.hevo.catalog.v2.core.vo.PrimaryKeyConstraint;
import io.hevo.catalog.v2.core.vo.SourceConstraints;
import io.hevo.catalog.v2.core.vo.SourceObjectAttributes;
import io.hevo.connector.cdk.model.ObjectLoadStrategy;
import io.hevo.connector.cdk.schema.ObjectSpec;
import io.hevo.connector.cdk.utils.CatalogFieldFactory;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Synthesizes a Hevo {@link ObjectSchema} from a stream's JSON schema (InlineSchemaLoader),
 * following the dynamic-schema pattern used by BambooHR's employee stream.
 *
 * <p>JSON-schema type → Hevo type: string→VARCHAR (date-time format→DATE_TIME_TZ, date→DATE),
 * integer→LONG, number→DOUBLE, boolean→BOOLEAN, object/array/unknown→JSON. Union types like
 * ["null","string"] mark the field nullable.
 */
public final class SchemaSynthesizer {

  private static final int DEFAULT_VARCHAR_LENGTH = 4096;

  private SchemaSynthesizer() {}

  public static ObjectSchema synthesize(StreamSpec stream) {
    Map<String, Object> jsonSchema = stream.jsonSchema();
    if (jsonSchema == null) {
      throw new ManifestException(
          "Stream " + stream.name() + " has no inline schema to synthesize from");
    }
    Map<String, Object> properties = Components.getMap(jsonSchema, "properties");
    if (properties == null || properties.isEmpty()) {
      throw new ManifestException("Stream " + stream.name() + " schema has no properties");
    }

    ObjectDetails objectDetails =
        ObjectDetails.builder()
            .catalog(null)
            .schema(null)
            .table(stream.name())
            .delimiter(null)
            .type("TABLE")
            .sourceObjectStatus(SourceObjectStatus.ACTIVE)
            .inActiveReason(null)
            .build();

    Set<Field> fields = new HashSet<>();
    int position = 0;
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      String fieldName = property.getKey();
      Map<String, Object> definition =
          property.getValue() instanceof Map<?, ?>
              ? Components.getMap(properties, fieldName)
              : Map.of();
      TypeInfo typeInfo = resolveType(definition);
      int pkIndex = stream.primaryKey().indexOf(fieldName);
      boolean isPk = pkIndex >= 0;
      fields.add(
          CatalogFieldFactory.createField(
              new CatalogFieldFactory.Params(
                  fieldName,
                  typeInfo.hevoType,
                  typeInfo.sourceType,
                  position++,
                  FieldState.ACTIVE,
                  null,
                  isPk ? pkIndex : null,
                  null,
                  !isPk && typeInfo.nullable,
                  null,
                  "DATE_TIME_TZ".equals(typeInfo.hevoType) ? 0 : null,
                  "VARCHAR".equals(typeInfo.hevoType) ? DEFAULT_VARCHAR_LENGTH : null,
                  null,
                  null)));
    }
    return new ObjectSchema(objectDetails, fields);
  }

  /**
   * Synthesizes a V2 {@link ObjectSpec} (SDP platform) from a stream's JSON schema — the dynamic
   * counterpart of {@code SchemaParserV2} parsing a hand-written schema.yml.
   *
   * <p>Load strategy is derived from manifest semantics: a primary key means records can be
   * merged/upserted; without one, incremental streams append and full-refresh streams replace the
   * table each run.
   */
  public static ObjectSpec synthesizeSpec(StreamSpec stream) {
    Map<String, Object> jsonSchema = stream.jsonSchema();
    if (jsonSchema == null) {
      throw new ManifestException(
          "Stream " + stream.name() + " has no inline schema to synthesize from");
    }
    Map<String, Object> properties = Components.getMap(jsonSchema, "properties");
    if (properties == null || properties.isEmpty()) {
      throw new ManifestException("Stream " + stream.name() + " schema has no properties");
    }

    Map<String, HField> fieldsByName = new LinkedHashMap<>();
    int position = 0;
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      String fieldName = property.getKey();
      Map<String, Object> definition =
          property.getValue() instanceof Map<?, ?>
              ? Components.getMap(properties, fieldName)
              : Map.of();
      TypeInfo typeInfo = resolveType(definition);
      int pkIndex = stream.primaryKey().indexOf(fieldName);
      boolean isPk = pkIndex >= 0;
      fieldsByName.put(
          fieldName,
          (HField)
              CatalogFieldFactory.createField(
                  new CatalogFieldFactory.Params(
                      fieldName,
                      typeInfo.hevoType(),
                      typeInfo.sourceType(),
                      position++,
                      FieldState.ACTIVE,
                      null,
                      isPk ? pkIndex : null,
                      null,
                      !isPk && typeInfo.nullable(),
                      null,
                      "DATE_TIME_TZ".equals(typeInfo.hevoType()) ? 0 : null,
                      "VARCHAR".equals(typeInfo.hevoType()) ? DEFAULT_VARCHAR_LENGTH : null,
                      null,
                      null)));
    }

    boolean incremental = stream.incrementalSync() != null;
    boolean hasPrimaryKey = !stream.primaryKey().isEmpty();
    ObjectLoadStrategy loadStrategy =
        hasPrimaryKey
            ? ObjectLoadStrategy.mergeWithoutDeletes()
            : incremental ? ObjectLoadStrategy.Append() : ObjectLoadStrategy.replace();

    return ObjectSpec.builder()
        .namespace(stream.namespace())
        .status(SourceObjectStatus.ACTIVE)
        .objectLoadStrategy(loadStrategy)
        .attributes(
            SourceObjectAttributes.builder()
                .canCaptureDeletes(false)
                .canCaptureSourceModifiedAt(incremental)
                .build())
        .constraints(
            SourceConstraints.of(
                PrimaryKeyConstraint.of(stream.primaryKey()), List.of(), List.of()))
        .fieldsByName(fieldsByName)
        .build();
  }

  /**
   * Resolved Hevo type per field name for a stream, in schema order. Used by {@link
   * RecordNormalizer} to coerce raw JSON values into the shapes the publish-time {@code
   * DataConverterUtils}/{@code HDatumUtils} casts expect.
   */
  public static Map<String, String> fieldHevoTypes(StreamSpec stream) {
    Map<String, Object> jsonSchema = stream.jsonSchema();
    Map<String, Object> properties =
        jsonSchema == null ? null : Components.getMap(jsonSchema, "properties");
    if (properties == null || properties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> types = new LinkedHashMap<>();
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      String fieldName = property.getKey();
      Map<String, Object> definition =
          property.getValue() instanceof Map<?, ?>
              ? Components.getMap(properties, fieldName)
              : Map.of();
      types.put(fieldName, resolveType(definition).hevoType());
    }
    return types;
  }

  private record TypeInfo(String hevoType, String sourceType, boolean nullable) {}

  private static TypeInfo resolveType(Map<String, Object> definition) {
    Object rawType = definition == null ? null : definition.get("type");
    boolean nullable = false;
    String jsonType = null;
    if (rawType instanceof String s) {
      jsonType = s;
    } else if (rawType instanceof List<?> types) {
      for (Object type : types) {
        if ("null".equals(type)) {
          nullable = true;
        } else if (jsonType == null) {
          jsonType = String.valueOf(type);
        }
      }
    } else {
      nullable = true;
    }
    String format = definition == null ? null : Components.getString(definition, "format");
    String hevoType =
        switch (jsonType == null ? "unknown" : jsonType) {
          case "string" -> "date-time".equals(format)
              ? "DATE_TIME_TZ"
              : "date".equals(format) ? "DATE" : "VARCHAR";
          case "integer" -> "LONG";
          case "number" -> "DOUBLE";
          case "boolean" -> "BOOLEAN";
          default -> "JSON"; // object, array, unknown
        };
    return new TypeInfo(hevoType, jsonType == null ? "unknown" : jsonType, nullable);
  }
}
