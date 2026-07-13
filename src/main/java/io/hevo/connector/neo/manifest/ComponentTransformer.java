package io.hevo.connector.neo.manifest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Recursively propagates {@code $parameters} from parent to child components and inserts default
 * {@code type} tags where the schema implies them.
 *
 * <p>Port of airbyte_cdk/sources/declarative/parsers/manifest_component_transformer.py. Semantics:
 *
 * <ul>
 *   <li>Missing {@code type} is inferred from the parent's {@code <ParentType>.<field>} identifier
 *       via {@link #DEFAULT_MODEL_TYPES} (or {@link #CUSTOM_COMPONENTS_MAPPING} when the component
 *       declares {@code class_name}).
 *   <li>{@code $parameters} at a component merge with parent parameters; the current level wins
 *       (unless {@code useParentParameters} is set).
 *   <li>Parameters become plain fields on the component, with existing fields taking precedence,
 *       and the merged parameter map is re-attached as {@code $parameters} for descendants.
 *   <li>JSON-schema objects ({@code type: object}) are left untouched.
 * </ul>
 */
public final class ComponentTransformer {

  private static final String PARAMETERS_KEY = "$parameters";

  static final Map<String, String> DEFAULT_MODEL_TYPES =
      Map.ofEntries(
          Map.entry("CompositeErrorHandler.error_handlers", "DefaultErrorHandler"),
          Map.entry("CursorPagination.decoder", "JsonDecoder"),
          Map.entry("DatetimeBasedCursor.end_datetime", "MinMaxDatetime"),
          Map.entry("DatetimeBasedCursor.end_time_option", "RequestOption"),
          Map.entry("DatetimeBasedCursor.start_datetime", "MinMaxDatetime"),
          Map.entry("DatetimeBasedCursor.start_time_option", "RequestOption"),
          Map.entry("DeclarativeSource.check", "CheckStream"),
          Map.entry("DeclarativeSource.spec", "Spec"),
          Map.entry("DeclarativeSource.streams", "DeclarativeStream"),
          Map.entry("DeclarativeStream.retriever", "SimpleRetriever"),
          Map.entry("DeclarativeStream.schema_loader", "JsonFileSchemaLoader"),
          Map.entry("DynamicDeclarativeStream.stream_template", "DeclarativeStream"),
          Map.entry("DynamicDeclarativeStream.components_resolver", "ConfigComponentResolver"),
          Map.entry("HttpComponentsResolver.retriever", "SimpleRetriever"),
          Map.entry("HttpComponentsResolver.components_mapping", "ComponentMappingDefinition"),
          Map.entry("ConfigComponentsResolver.stream_config", "StreamConfig"),
          Map.entry("ConfigComponentsResolver.components_mapping", "ComponentMappingDefinition"),
          Map.entry("DefaultErrorHandler.response_filters", "HttpResponseFilter"),
          Map.entry("DefaultPaginator.decoder", "JsonDecoder"),
          Map.entry("DefaultPaginator.page_size_option", "RequestOption"),
          Map.entry("DpathExtractor.decoder", "JsonDecoder"),
          Map.entry("DpathExtractor.record_expander", "RecordExpander"),
          Map.entry("HttpRequester.error_handler", "DefaultErrorHandler"),
          Map.entry("ListPartitionRouter.request_option", "RequestOption"),
          Map.entry("ParentStreamConfig.request_option", "RequestOption"),
          Map.entry("ParentStreamConfig.stream", "DeclarativeStream"),
          Map.entry("RecordSelector.extractor", "DpathExtractor"),
          Map.entry("RecordSelector.record_filter", "RecordFilter"),
          Map.entry("SimpleRetriever.paginator", "NoPagination"),
          Map.entry("SimpleRetriever.record_selector", "RecordSelector"),
          Map.entry("SimpleRetriever.requester", "HttpRequester"),
          Map.entry("SubstreamPartitionRouter.parent_stream_configs", "ParentStreamConfig"),
          Map.entry("AddFields.fields", "AddedFieldDefinition"),
          Map.entry("CustomPartitionRouter.parent_stream_configs", "ParentStreamConfig"),
          Map.entry("DynamicSchemaLoader.retriever", "SimpleRetriever"),
          Map.entry("SchemaTypeIdentifier.types_map", "TypesMap"));

  static final Map<String, String> CUSTOM_COMPONENTS_MAPPING =
      Map.ofEntries(
          Map.entry("CompositeErrorHandler.backoff_strategies", "CustomBackoffStrategy"),
          Map.entry("DeclarativeStream.retriever", "CustomRetriever"),
          Map.entry("DeclarativeStream.transformations", "CustomTransformation"),
          Map.entry("DefaultErrorHandler.backoff_strategies", "CustomBackoffStrategy"),
          Map.entry("DefaultPaginator.pagination_strategy", "CustomPaginationStrategy"),
          Map.entry("HttpRequester.authenticator", "CustomAuthenticator"),
          Map.entry("HttpRequester.error_handler", "CustomErrorHandler"),
          Map.entry("RecordSelector.extractor", "CustomRecordExtractor"),
          Map.entry("SimpleRetriever.partition_router", "CustomPartitionRouter"));

  public Map<String, Object> propagateTypesAndParameters(
      String parentFieldIdentifier,
      Map<String, Object> declarativeComponent,
      Map<String, Object> parentParameters) {
    return propagateTypesAndParameters(
        parentFieldIdentifier, declarativeComponent, parentParameters, null);
  }

  public Map<String, Object> propagateTypesAndParameters(
      String parentFieldIdentifier,
      Map<String, Object> declarativeComponent,
      Map<String, Object> parentParameters,
      Boolean useParentParameters) {
    Map<String, Object> component = deepCopyMap(declarativeComponent);

    if (!component.containsKey("type")) {
      String foundType =
          component.containsKey("class_name")
              ? CUSTOM_COMPONENTS_MAPPING.get(parentFieldIdentifier)
              : DEFAULT_MODEL_TYPES.get(parentFieldIdentifier);
      if (foundType != null) {
        component.put("type", foundType);
      }
    }

    // Current-level $parameters merge with the parent's; current level wins unless
    // useParentParameters is set.
    Map<String, Object> currentParameters = deepCopyMap(parentParameters);
    Object rawComponentParameters = component.remove(PARAMETERS_KEY);
    Map<String, Object> componentParameters =
        rawComponentParameters instanceof Map<?, ?> map ? castMap(map) : new LinkedHashMap<>();
    if (Boolean.TRUE.equals(useParentParameters)) {
      Map<String, Object> merged = new LinkedHashMap<>(componentParameters);
      merged.putAll(currentParameters);
      currentParameters = merged;
    } else {
      currentParameters.putAll(componentParameters);
    }

    if (isJsonSchemaObject(component)) {
      return component;
    }

    if (!component.containsKey("type")) {
      if (hasNestedComponents(component)) {
        processNestedComponents(
            component, parentFieldIdentifier, currentParameters, useParentParameters);
      }
      return component;
    }

    // Parameters become fields on the component; existing fields take precedence. Matches
    // Python's `get(key) or value`: falsy existing values (null, "", 0, false, empty
    // collections) are overwritten by the parameter.
    for (Map.Entry<String, Object> parameter : currentParameters.entrySet()) {
      Object existing = component.get(parameter.getKey());
      if (isFalsy(existing)) {
        component.put(parameter.getKey(), parameter.getValue());
      }
    }

    for (Map.Entry<String, Object> field : component.entrySet()) {
      String fieldName = field.getKey();
      Object fieldValue = field.getValue();
      String childIdentifier = component.get("type") + "." + fieldName;
      if (fieldValue instanceof Map<?, ?> mapValue) {
        // Exclude a parameter matching the field name to avoid an infinite cycle.
        Object excluded = currentParameters.remove(fieldName);
        field.setValue(
            propagateTypesAndParameters(
                childIdentifier, castMap(mapValue), currentParameters, useParentParameters));
        if (!isFalsy(excluded)) {
          currentParameters.put(fieldName, excluded);
        }
      } else if (fieldValue instanceof List<?> listValue) {
        Object excluded = currentParameters.remove(fieldName);
        List<Object> newList = new ArrayList<>(listValue.size());
        for (Object element : listValue) {
          if (element instanceof Map<?, ?> elementMap) {
            newList.add(
                propagateTypesAndParameters(
                    childIdentifier, castMap(elementMap), currentParameters, useParentParameters));
          } else {
            newList.add(element);
          }
        }
        field.setValue(newList);
        if (!isFalsy(excluded)) {
          currentParameters.put(fieldName, excluded);
        }
      }
    }

    if (!currentParameters.isEmpty()) {
      component.put(PARAMETERS_KEY, currentParameters);
    }
    return component;
  }

  private static boolean isJsonSchemaObject(Map<String, Object> component) {
    Object type = component.get("type");
    return "object".equals(type) || List.of("null", "object").equals(type);
  }

  private static boolean hasNestedComponents(Map<String, Object> component) {
    for (Object value : component.values()) {
      if (value instanceof Map<?, ?> map && map.get("type") != null) {
        return true;
      }
    }
    return false;
  }

  private void processNestedComponents(
      Map<String, Object> component,
      String parentFieldIdentifier,
      Map<String, Object> currentParameters,
      Boolean useParentParameters) {
    for (Map.Entry<String, Object> field : component.entrySet()) {
      if (field.getValue() instanceof Map<?, ?> map && map.get("type") != null) {
        field.setValue(
            propagateTypesAndParameters(
                parentFieldIdentifier, castMap(map), currentParameters, useParentParameters));
      }
    }
  }

  /** Python truthiness for the parameter-overlay rule. */
  private static boolean isFalsy(Object value) {
    return value == null
        || Boolean.FALSE.equals(value)
        || "".equals(value)
        || (value instanceof Number n && n.doubleValue() == 0.0)
        || (value instanceof Map<?, ?> m && m.isEmpty())
        || (value instanceof List<?> l && l.isEmpty());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castMap(Map<?, ?> map) {
    return (Map<String, Object>) map;
  }

  private static Map<String, Object> deepCopyMap(Map<String, Object> map) {
    Map<String, Object> copy = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      copy.put(entry.getKey(), deepCopyValue(entry.getValue()));
    }
    return copy;
  }

  private static Object deepCopyValue(Object value) {
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> copy = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        copy.put(String.valueOf(entry.getKey()), deepCopyValue(entry.getValue()));
      }
      return copy;
    }
    if (value instanceof List<?> list) {
      List<Object> copy = new ArrayList<>(list.size());
      for (Object item : list) {
        copy.add(deepCopyValue(item));
      }
      return copy;
    }
    return value;
  }
}
