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
package io.cdap.plugin.ariba.source.metadata.proto;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Response structure for simple fields
 */
public class SimpleFields {

  @SerializedName("allowedValues")
  private Object allowedValues;

  @SerializedName("size")
  private Integer size;

  @SerializedName("precision")
  private Integer precision;

  @SerializedName("scale")
  private Integer scale;

  @SerializedName("title")
  private String title;

  @SerializedName("type")
  private List<String> type;

  @SerializedName("isPrimaryKey")
  private boolean isPrimaryKey;

  public Object getAllowedValues() {
    return allowedValues;
  }

  public Integer getSize() {
    return size;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  public String getTitle() {
    return title;
  }

  public List<String> getType() {
    return type;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }
}
