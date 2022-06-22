/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.ariba.source.metadata;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This {@code AribaSchemaGenerator} contains all the logic to generate schemas.
 */
public class AribaSchemaGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(AribaSchemaGenerator.class);

  // Mapping of Ariba type as key and its corresponding Schema type as value
  private static final Map<String, Schema> SCHEMA_TYPE_MAPPING;

  static {
    Map<String, Schema> dataTypeMap = new HashMap<>();
    dataTypeMap.put("number", Schema.of(Schema.Type.DOUBLE));
    dataTypeMap.put("boolean", Schema.of(Schema.Type.BOOLEAN));
    dataTypeMap.put("string", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("date", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));

    SCHEMA_TYPE_MAPPING = Collections.unmodifiableMap(dataTypeMap);
  }

  private final List<AribaColumnMetadata> aribaColumnMetadata;

  public AribaSchemaGenerator(List<AribaColumnMetadata> aribaColumnMetadata) {
    this.aribaColumnMetadata = aribaColumnMetadata;
  }


  /**
   * Builds schema from the given list of {@code AribaColumnMetadata}
   *
   * @return {@code Schema}
   */
  public Schema buildSchema() {
    List<Schema.Field> schema = aribaColumnMetadata.stream()
      .map(this::buildSchemaField)
      .collect(Collectors.toList());
    LOG.trace("Finished creating Schema Field for metadata.");
    return Schema.recordOf("AribaColumnMetadata", schema);
  }


  /**
   * Builds Schema field from {@code AribaColumnMetadata}
   *
   * @return {@code Schema.Field}
   */
  @VisibleForTesting
  protected Schema.Field buildSchemaField(AribaColumnMetadata columnDetails) {
    if (columnDetails.getChildList() != null && !columnDetails.getChildList().isEmpty()) {
      List<Schema.Field> outputSchema = columnDetails.getChildList()
        .stream()
        .map(this::buildSchemaField)
        .collect(Collectors.toList());

      if (columnDetails.getType().equalsIgnoreCase(ResourceConstants.ARRAY)) {
        return Schema.Field.of(columnDetails.getName(),
                               Schema.nullableOf((Schema.arrayOf(Schema.recordOf(columnDetails.getName(),
                                                                                 outputSchema)))));
      }
      return Schema.Field.of(columnDetails.getName(),
                             Schema.nullableOf(Schema.recordOf(columnDetails.getName(), outputSchema)));

    }
    return Schema.Field.of(columnDetails.getName(), buildSchemaType(columnDetails));
  }

  /**
   * Build and returns the appropriate schema type.
   */
  private Schema buildSchemaType(AribaColumnMetadata columnDetails) {
    Schema schemaType = SCHEMA_TYPE_MAPPING.get(columnDetails.getType());
    if (schemaType != null) {
      return columnDetails.isPrimaryKey() ? schemaType : Schema.nullableOf(schemaType);
    } else if (columnDetails.getType().equalsIgnoreCase(ResourceConstants.ARRAY) &&
      Objects.requireNonNull(columnDetails.getChildList()).isEmpty()) {
      return Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    } else {
      return Schema.nullableOf(Schema.of(Schema.Type.STRING));
    }
  }

}
