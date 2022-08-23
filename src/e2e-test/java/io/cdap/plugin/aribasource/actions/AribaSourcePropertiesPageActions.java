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

package io.cdap.plugin.aribasource.actions;

import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.aribasource.locators.AribaSourcePropertiesPage;
import io.cdap.plugin.tests.hooks.TestSetupHooks;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Represents - Ariba - Source plugin - Properties page - Actions.
 */
public class AribaSourcePropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(AribaSourcePropertiesPageActions.class);
  private static Gson gson = new Gson();

  public static void verifyExpectedDateFormatFromPlaceholder(String datacyValue) {
    String actualText = ElementHelper.getElementAttribute(AribaSourcePropertiesPage.getWebElement(datacyValue),
                                                          "placeholder");
    String expectedText = PluginPropertyUtils.errorProp("message." + datacyValue);

    Assert.assertEquals(expectedText, actualText);
  }

  public static void verifyIfRecordCreatedInSinkIsCorrect(String aribaRecordJsonArray) throws IOException, InterruptedException {
    JsonArray aribaRecordArray = gson.fromJson(aribaRecordJsonArray, JsonArray.class);
    String aribaRecord = aribaRecordArray.get(0).toString();
    JsonObject aribaJsonObject = gson.fromJson(aribaRecordArray.get(0), JsonObject.class);
    long uniqueId = aribaJsonObject.get("id").getAsLong();

    TableResult bigQueryTableData = getBigQueryTableData(TestSetupHooks.bqTargetDataset, TestSetupHooks.bqTargetTable,
                                                         uniqueId);
    if (bigQueryTableData == null) {
      return;
    }
    String bigQueryRecord = bigQueryTableData.getValues().iterator().next().get(0).getValue().toString();
    Assert.assertTrue(compareValueOfBothJson(aribaRecord, bigQueryRecord));
  }

  private static boolean compareValueOfBothJson(String aribaResponse, String bigQueryResponse) {
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();

    Map<String, Object> aribaResponseInmap = gson.fromJson(aribaResponse, type);
    Map<String, Object> bigQueryResponseInMap = gson.fromJson(bigQueryResponse, type);
    MapDifference<String, Object> mapDifference = Maps.difference(aribaResponseInmap, bigQueryResponseInMap);

    return mapDifference.areEqual();
  }

  public static TableResult getBigQueryTableData(String dataset, String table, long uniqueId)
    throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    //Query can be changed based on the record in Ariba
    String selectQuery = "SELECT TO_JSON(t) result FROM `" + projectId + "." + dataset + "." + table +
      "` AS t WHERE id=" + uniqueId + "";

    return BigQueryClient.getQueryResult(selectQuery);
  }
}
