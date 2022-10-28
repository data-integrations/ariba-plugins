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
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.aribasource.locators.AribaSourcePropertiesPage;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Represents - Ariba - Source plugin - Properties page - Actions.
 */
public class AribaSourcePropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(AribaSourcePropertiesPageActions.class);
  private static Gson gson = new Gson();
  private static String projectId = PluginPropertyUtils.pluginProp("projectId");
  private static List<String> bigQueryrows = new ArrayList<>();
  public static void verifyExpectedDateFormatFromPlaceholder(String datacyValue) {
    String actualText = ElementHelper.getElementAttribute(
      AribaSourcePropertiesPage.getWebElement(datacyValue), "placeholder");
    String expectedText = PluginPropertyUtils.errorProp("message." + datacyValue);

    logger.info("Verifying that the element: " + AribaSourcePropertiesPage.getWebElement(datacyValue) +
                  " contains text: " + expectedText);
    logger.info("Actual text: " + actualText);
    Assert.assertEquals(expectedText, actualText);
  }

  public static void verifyIfRecordCreatedInSinkForViewTemplateIsCorrect(String expectedOutputFile)
    throws IOException, InterruptedException {
    List<String> expectedOutput = new ArrayList<>();
     try (BufferedReader bf1 = Files.newBufferedReader(Paths.get(PluginPropertyUtils.pluginProp(expectedOutputFile)))) {
      String line;
      while ((line = bf1.readLine()) != null) {
        expectedOutput.add(line);
      }
    }

     createNewTableFromQuery(PluginPropertyUtils.pluginProp("dataset"),
                             PluginPropertyUtils.pluginProp("bqtarget.table"));
     for (int expectedRow = 0; expectedRow < expectedOutput.size(); expectedRow++) {
      JsonObject expectedOutputAsJson = gson.fromJson(expectedOutput.get(expectedRow), JsonObject.class);
      String uniqueId = expectedOutputAsJson.get("ProjectId").getAsString();
      getBigQueryTableData(PluginPropertyUtils.pluginProp("dataset"),
                           PluginPropertyUtils.pluginProp("bqtarget.table"), uniqueId);
     }
     for (int row = 0; row < bigQueryrows.size(); row++) {
      Assert.assertTrue(compareValueOfBothResponses(expectedOutput.get(row), bigQueryrows.get(row)));
    }
     bigQueryrows.clear();
  }

  static boolean compareValueOfBothResponses(String aribaResponse, String bigQueryResponse) {
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    Map<String, Object> aribaResponseInmap = gson.fromJson(aribaResponse, type);
    Map<String, Object> bigQueryResponseInMap = gson.fromJson(bigQueryResponse, type);
    MapDifference<String, Object> mapDifference = Maps.difference(aribaResponseInmap, bigQueryResponseInMap);
    logger.info("Record of source and sink application is :" + mapDifference);

    return mapDifference.areEqual();
  }

  public static void getBigQueryTableData(String dataset, String table, String uniqueId)
    throws IOException, InterruptedException {
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "_v1` AS t WHERE " +
      "ProjectId='" + uniqueId + "'";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryrows.add(value.get(0).getValue().toString()));
  }

  public static void createNewTableFromQuery(String dataset, String table)
    throws IOException, InterruptedException {
    String selectQuery = "Create table " + dataset + "." + table + "_v1 AS SELECT * EXCEPT (LoadUpdateTime) FROM" +
      " `" + projectId + "." + dataset + "." + table + "`";
    BigQueryClient.executeQuery(selectQuery);
  }

  public static void customTimeout(int timeoutInSeconds) throws InterruptedException {
    TimeUnit time = TimeUnit.SECONDS;
    time.sleep(timeoutInSeconds);
 }
}
