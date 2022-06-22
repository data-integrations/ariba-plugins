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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.cdap.plugin.ariba.source.util.ResourceConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Get Properties for all type of fields
 */
public class Properties {
  private static final Gson GSON = new Gson();

  public static Map<String, ObjectFields> getObjectFields(JsonElement properties) {
    Map<String, ObjectFields> retMapMain = new HashMap<>();
    Map<String, ObjectFields> retMap =
      GSON.fromJson(properties.toString(), new TypeToken<HashMap<String, ObjectFields>>() {
                    }.getType()
      );
    for (Map.Entry<String, ObjectFields> entry : retMap.entrySet()) {
      if (retMap.get(entry.getKey()).getType().contains(ResourceConstants.OBJECT)) {
        retMapMain.put(entry.getKey(), entry.getValue());
      }
    }
    return retMapMain;
  }

  public static Map<String, ArrayFields> getArrayFields(JsonElement properties) {
    Map<String, ArrayFields> retMapMain = new HashMap<>();
    Map<String, ArrayFields> retMap =
      GSON.fromJson(properties.toString(), new TypeToken<HashMap<String, ArrayFields>>() {
                    }.getType()
      );
    for (Map.Entry<String, ArrayFields> entry : retMap.entrySet()) {
      if (retMap.get(entry.getKey()).getType().contains(ResourceConstants.ARRAY)) {
        retMapMain.put(entry.getKey(), entry.getValue());
      }
    }
    return retMapMain;
  }

  public static Map<String, SimpleFields> getNonObjectFields(JsonElement properties) {
    Map<String, SimpleFields> retMapMain = new HashMap<>();
    Map<String, SimpleFields> retMap =
      GSON.fromJson(properties.toString(), new TypeToken<HashMap<String, SimpleFields>>() {
      }.getType());
    for (Map.Entry<String, SimpleFields> entry : retMap.entrySet()) {
      if (!(retMap.get(entry.getKey()).getType().contains(ResourceConstants.OBJECT) ||
        retMap.get(entry.getKey()).getType().contains(ResourceConstants.ARRAY))) {
        retMapMain.put(entry.getKey(), entry.getValue());
      }
    }
    return retMapMain;
  }

}
