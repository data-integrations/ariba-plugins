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

import io.cdap.cdap.api.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This {@code AribaSchemaGenerator} contains all the logic to generate schemas.
 */
public class AribaSchemaGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(AribaSchemaGenerator.class);

  // Mapping of Ariba type as key and its corresponding Schema type as value
  private static final Map<String, Schema> SCHEMA_TYPE_MAPPING;

  static {
    Map<String, Schema> dataTypeMap = new HashMap<>();
    dataTypeMap.put("number", Schema.of(Schema.Type.INT));
    dataTypeMap.put("boolean", Schema.of(Schema.Type.BOOLEAN));
    dataTypeMap.put("string", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("date", Schema.of(Schema.LogicalType.DATE));
    dataTypeMap.put("TimeDim", Schema.of(Schema.LogicalType.DATE));
    dataTypeMap.put("UserDataDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("ProjectInfo", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("ProcessDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("CommodityDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("OrganizationDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("RegionDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("SourceSystemDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("EventTypeDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("SupplierDim", Schema.of(Schema.Type.STRING));
    dataTypeMap.put("ProjectInfoDim", Schema.of(Schema.Type.STRING));

    SCHEMA_TYPE_MAPPING = Collections.unmodifiableMap(dataTypeMap);
  }

  private final AribaColumnMetadata aribaColumnMetadata;

  public AribaSchemaGenerator(AribaColumnMetadata aribaColumnMetadata) {
    this.aribaColumnMetadata = aribaColumnMetadata;
  }


  /**
   * Builds schema from the given list of {@code AribaColumnMetadata}
   *
   * @return {@code Schema}
   */
  public Schema buildSchema() {
    List<Schema.Field> schema = aribaColumnMetadata.getColumnDetails().stream()
      .map(this::buildSchemaField)
      .collect(Collectors.toList());

    LOGGER.trace("Finished creating Schema Field for metadata.");

    return Schema.recordOf("AribaColumnMetadata", schema);
  }


  /**
   * Builds Schema field from {@code AribaColumnMetadata}
   *
   * @return {@code Schema.Field}
   */
  private Schema.Field buildSchemaField(AribaColumnMetadata.ColumnDetails columnDetails) {
    String encodeName = columnDetails.getName().replace(".", "__");
    return Schema.Field.of(encodeName, buildRequiredSchemaType(columnDetails));
  }

  /**
   * Build and returns the appropriate schema type.
   */
  private Schema buildRequiredSchemaType(AribaColumnMetadata.ColumnDetails columnDetails) {
    if (SCHEMA_TYPE_MAPPING.get(columnDetails.getType()) != null) {
      return SCHEMA_TYPE_MAPPING.get(columnDetails.getType());
    } else {
      return Schema.of(Schema.Type.STRING);
    }
  }

}
