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

import java.util.Collections;
import java.util.List;

/**
 * Ariba Column metadata details
 */
public class AribaColumnMetadata {

  public static final String SELECT_FIELDS = "selectFields";
  public static final String NAME = "name";
  public static final String TYPE = "type";
  public static final String SIZE = "size";
  public static final String ALLOWED_VALUES = "allowedValues";
  public static final String IS_CUSTOM_FIELD = "isCustomField";
  public static final String PRECISION = "precision";
  public static final String SCALE = "scale";
  private final String documentType;
  private final List<ColumnDetails> columnDetails;
  private List<AribaColumnMetadata> childList;

  public AribaColumnMetadata(String documentType, List<ColumnDetails> columnDetails) {
    this.documentType = documentType;
    this.columnDetails = columnDetails;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getDocumentType() {
    return documentType;
  }

  public List<ColumnDetails> getColumnDetails() {
    return Collections.unmodifiableList(columnDetails);
  }

  /**
   * Column Details
   */
  public static class ColumnDetails {
    private final String name;
    private final String type;
    private final int size;
    private final String allowedValues;
    private final boolean isCustomField;
    private final int precision;
    private final int scale;

    public ColumnDetails(String name, String type, int size, String allowedValues,
                         boolean isCustomField, int precision, int scale) {
      this.name = name;
      this.type = type;
      this.size = size;
      this.allowedValues = allowedValues;
      this.isCustomField = isCustomField;
      this.precision = precision;
      this.scale = scale;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public int getSize() {
      return size;
    }

    public String getAllowedValues() {
      return allowedValues;
    }

    public boolean isCustomField() {
      return isCustomField;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }
  }

  /**
   * Helper class to simplify {@link AribaColumnMetadata} class creation.
   */
  public static class Builder {
    private String documentType;
    private List<ColumnDetails> columnDetails;

    public Builder documentType(String documentType) {
      this.documentType = documentType;
      return this;
    }

    public Builder columnDetails(List<ColumnDetails> columnDetails) {
      this.columnDetails = columnDetails;
      return this;
    }

    public AribaColumnMetadata build() {
      return new AribaColumnMetadata(this.documentType, this.columnDetails);
    }
  }
}
