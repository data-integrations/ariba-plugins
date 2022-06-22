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

import java.util.List;
import javax.annotation.Nullable;

/**
 * Ariba Column metadata details
 */
public class AribaColumnMetadata {

  private final String viewTemplateName;
  private final String name;
  private final String type;
  private final int size;
  private final boolean isCustomField;
  private final int precision;
  private final int scale;
  private final boolean isPrimaryKey;
  private final List<AribaColumnMetadata> childList;

  public AribaColumnMetadata(String viewTemplateName, String name, String type, int size,
                             boolean isCustomField,
                             int precision, int scale, boolean isPrimaryKey,
                             List<AribaColumnMetadata> childList) {
    this.viewTemplateName = viewTemplateName;
    this.name = name;
    this.type = type;
    this.size = size;
    this.isCustomField = isCustomField;
    this.precision = precision;
    this.scale = scale;
    this.isPrimaryKey = isPrimaryKey;
    this.childList = childList;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getViewTemplateName() {
    return viewTemplateName;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Nullable
  public int getSize() {
    return size;
  }

  @Nullable
  public boolean isCustomField() {
    return isCustomField;
  }

  @Nullable
  public int getPrecision() {
    return precision;
  }

  @Nullable
  public int getScale() {
    return scale;
  }

  @Nullable
  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  @Nullable
  public List<AribaColumnMetadata> getChildList() {
    return childList;
  }

  /**
   * Helper class to simplify {@link AribaColumnMetadata} class creation.
   */
  public static class Builder {
    private String viewTemplateName;
    private String name;
    private String type;
    private Integer size;
    private Boolean isCustomField;
    private int precision;
    private int scale;
    private boolean isPrimaryKey;
    private List<AribaColumnMetadata> childList;

    public Builder viewTemplateName(String viewTemplateName) {
      this.viewTemplateName = viewTemplateName;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder size(@Nullable Integer size) {
      this.size = size;
      return this;
    }

    public Builder type(String type) {
      this.type = type;
      return this;
    }

    public Builder isCustomField(Boolean isCustomField) {
      this.isCustomField = isCustomField;
      return this;
    }

    public Builder precision(@Nullable Integer precision) {
      this.precision = precision;
      return this;
    }

    public Builder scale(@Nullable Integer scale) {
      this.scale = scale;
      return this;
    }

    public Builder isPrimaryKey(@Nullable Boolean isPrimaryKey) {
      this.isPrimaryKey = isPrimaryKey;
      return this;
    }

    public Builder childList(@Nullable List<AribaColumnMetadata> childList) {
      this.childList = childList;
      return this;
    }

    public AribaColumnMetadata build() {
      return new AribaColumnMetadata(this.viewTemplateName, this.name, this.type, this.size, this.isCustomField,
                                     this.precision, this.scale, this.isPrimaryKey, this.childList);
    }
  }
}
