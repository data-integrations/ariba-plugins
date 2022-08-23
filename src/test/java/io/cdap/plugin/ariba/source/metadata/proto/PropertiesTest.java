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

import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.ariba.source.AribaBatchSource;
import io.cdap.plugin.ariba.source.AribaServices;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.connector.AribaConnector;
import io.cdap.plugin.ariba.source.connector.AribaConnectorConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.metadata.AribaColumnMetadata;
import io.cdap.plugin.ariba.source.metadata.AribaResponseContainer;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import io.cdap.plugin.common.ConfigUtil;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertiesTest {

  private static final Set<String> SUPPORTED_TYPES = new HashSet<>(Arrays.asList("TABLE", "VIEW"));
  private final Gson gson = new Gson();
  @Mocked
  AribaResponseContainer response;
  String jsonNode = "{\n" +
    "  \"type\": \"object\",\n" +
    "  \"access_token\": \"jiuokiopu\",\n" +
    "  \"status\": \"completed\",\n" +
    "  \"totalNumOfPages\": \"1\",\n" +
    "  \"currentPageNum\": \"1\",\n" +
    "  \"message\": \"test\",\n" +
    "  \"properties\": {\n" +
    "    \"IsTestProject\": {\n" +
    "      \"title\": \"IsTestProject\",\n" +
    "      \"type\": [\n" +
    "        \"boolean\",\n" +
    "        \"null\"\n" +
    "      ],\n" +
    "      \"precision\": null,\n" +
    "      \"scale\": null,\n" +
    "      \"size\": null,\n" +
    "      \"allowedValues\": null\n" +
    "    },\n" +
    "   \"Owner\": {\n" +
    "      \"type\": [\n" +
    "        \"object\",\n" +
    "        \"null\"\n" +
    "      ],\n" +
    "      \"properties\": {\n" +
    "        \"SourceSystem\": {\n" +
    "          \"title\": \"Owner.SourceSystem\",\n" +
    "          \"type\": [\n" +
    "            \"string\",\n" +
    "            \"null\"\n" +
    "          ],\n" +
    "          \"precision\": null,\n" +
    "          \"scale\": null,\n" +
    "          \"size\": 100,\n" +
    "          \"allowedValues\": null\n" +
    "        },\n" +
    "        \"UserId\": {\n" +
    "          \"title\": \"Owner.UserId\",\n" +
    "          \"type\": [\n" +
    "            \"string\",\n" +
    "            \"null\"\n" +
    "          ],\n" +
    "          \"precision\": null,\n" +
    "          \"scale\": null,\n" +
    "          \"size\": 50,\n" +
    "          \"allowedValues\": null\n" +
    "        }\n" +
    "      }\n" +
    "    },\n" +
    "     \"Organization\": {\n" +
    "      \"type\": [\n" +
    "        \"array\",\n" +
    "        \"null\"\n" +
    "      ],\n" +
    "      \"items\": [\n" +
    "        {\n" +
    "          \"type\": [\n" +
    "            \"object\",\n" +
    "            \"null\"\n" +
    "          ],\n" +
    "          \"properties\": {}\n" +
    "        },\n" +
    "         {\n" +
    "          \"type\": [\n" +
    "            \"array\",\n" +
    "            \"null\"\n" +
    "          ],\n" +
    "          \n" +
    "  \"items\": [\n" +
    "        {\n" +
    "          \"type\": [\n" +
    "            \"object\",\n" +
    "            \"null\"\n" +
    "          ],\n" +
    "          \"properties\": {}\n" +
    "        },\n" +
    "        {\n" +
    "          \"type\": [\n" +
    "            \"array\",\n" +
    "            \"null\"\n" +
    "          ],\n" +
    "          \"properties\": {}\n" +
    "        }\n" +
    "      ]\n" +
    "        }\n" +
    "        \n" +
    "      ]\n" +
    "    }\n" +
    "  }\n" +
    "}";
  private AribaPluginConfig pluginConfig;
  private Properties properties;
  private AribaServices aribaServices;

  @Before
  public void setUp() {
    properties = new Properties();
    pluginConfig = new AribaPluginConfig("unit-test-ref-name", "https://openapi.au.cloud.ariba.com",
                                         "prod", "CloudsufiDSAPP-T",
                                         "SourcingProjectFactSystemView", "08ee0299-4849-42a4-8464-3abed75fc74e",
                                         "c3B5wvrEsjKucFGlGhKSWUDqDRGE2Wds", "xryi0757SU8pEyk7ePc7grc7vgDXdz8O",
                                         "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
  }

  @Test
  public void testGetObjectFieldCount() {
    String objectJson = "{\"type\":\"object\",\"properties\":{\"SourceSystem\":{\"type\":" +
      "[\"object\",\"null\"]," +
      "\"properties\":{\"SourceSystemId\":{\"title\":\"SourceSystem.SourceSystemId\"," +
      "\"type\":[\"string\"," +
      "\"null\"],\"precision\":null,\"scale\":null,\"size\":100,\"allowedValues\":null}}}}}";

    AribaMetaResponse aribaMetaResponse = gson.fromJson(objectJson, AribaMetaResponse.class);

    Map<String, ObjectFields> objectFieldsMap = Properties.getObjectFields(aribaMetaResponse.getProperties());
    Assert.assertEquals("SourceSystem.SourceSystemId",
                        objectFieldsMap.get("SourceSystem").getProperties().getAsJsonObject()
                          .get("SourceSystemId").getAsJsonObject().get("title").getAsString());
  }

  @Test
  public void testGetObjectFieldValue() {
    String objectJson = "{\"type\":\"object\",\"properties\":{\"SourceSystem\":{\"type\"" +
      ":[\"object\",\"null\"]," +
      "\"properties\":{\"SourceSystemId\":{\"title\":\"SourceSystem.SourceSystemId\"" +
      " ,\"type\":[\"string\"," +
      "\"null\"],\"precision\":null,\"scale\":null,\"size\":100,\"allowedValues\":null}}}}}";
    AribaMetaResponse aribaMetaResponse = gson.fromJson(objectJson, AribaMetaResponse.class);
    Map<String, ObjectFields> objectFieldsMap = Properties.getObjectFields(aribaMetaResponse.getProperties());
    Assert.assertEquals("SourceSystem.SourceSystemId", objectFieldsMap.get("SourceSystem").getProperties()
      .getAsJsonObject().get("SourceSystemId").getAsJsonObject().get("title").getAsString());
    Assert.assertEquals("object", aribaMetaResponse.getType());
  }

  @Test
  public void testGetArrayFields() {
    String arrayJson = "{\"type\":\"object\",\"properties\":" +
      "{\"Client\":{\"type\":[\"array\",\"null\"],\"items\"" +
      ":[{\"type\":[\"object\",\"null\"],\"properties\":{\"DepartmentID\"" +
      ":{\"title\":\"Client.DepartmentID\"," +
      "\"type\":[\"string\",\"null\"],\"precision\":null,\"scale\":null,\"" +
      "size\":50,\"allowedValues\":null}," +
      "\"Description\":{\"title\":\"Client.Description\",\"type\":[\"string\",\"null\"]," +
      "\"precision\":null," +
      "\"scale\":null,\"size\":510,\"allowedValues\":null}}}]}}}";
    AribaMetaResponse aribaMetaResponse = gson.fromJson(arrayJson, AribaMetaResponse.class);
    Map<String, ArrayFields> arrayFieldsMap = Properties.getArrayFields(aribaMetaResponse.getProperties());
    Assert.assertEquals(1, arrayFieldsMap.size());
    Assert.assertEquals(50, arrayFieldsMap.get("Client").getItems()
      .getAsJsonArray().get(0).getAsJsonObject().get("properties")
      .getAsJsonObject().get("DepartmentID").getAsJsonObject().get("size").getAsInt());

  }

  @Test
  public void testGetNonObjectFields() {
    String simpleJson = "{\"type\":\"object\",\"properties\":{\"IsTestProject\":" +
      "{\"title\":\"IsTestProject\"," +
      "\"type\":[\"boolean\",\"null\"],\"precision\"" +
      ":1,\"scale\":2,\"size\":5,\"allowedValues\":7}}}";
    AribaMetaResponse aribaMetaResponse = gson.fromJson(simpleJson, AribaMetaResponse.class);
    AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();
    columnDetail.viewTemplateName(pluginConfig.getViewTemplateName())
      .name("name").isPrimaryKey(false).type(ResourceConstants.OBJECT).size(0)
      .isCustomField(false).scale(0).precision(0)
      .childList(null);
    Map<String, SimpleFields> simpleFieldsMap = Properties.getNonObjectFields(aribaMetaResponse.getProperties());
    Assert.assertEquals(1, simpleFieldsMap.size());
    Assert.assertEquals("IsTestProject", simpleFieldsMap.get("IsTestProject").getTitle());
    Assert.assertEquals(1, simpleFieldsMap.get("IsTestProject").getPrecision().intValue());
    Assert.assertFalse(simpleFieldsMap.get("IsTestProject").isPrimaryKey());
    Assert.assertEquals("boolean", simpleFieldsMap.get("IsTestProject").getType().get(0));
    Assert.assertEquals("2", simpleFieldsMap.get("IsTestProject").getScale().toString());
    Assert.assertEquals("7.0", simpleFieldsMap.get("IsTestProject").getAllowedValues().toString());
    Assert.assertEquals(5, simpleFieldsMap.get("IsTestProject").getSize().intValue());
  }

  @Test
  public void teatValidateFields() throws AribaException, IOException {
    MockFailureCollector collector = new MockFailureCollector();
    AribaConnectorConfig connectorConfig = pluginConfig.getConnection();
    connectorConfig.validateToken(collector);
    collector.getOrThrowException();
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private void testTest(AribaConnector connector) {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(context);
    ValidationException validationException = context.getFailureCollector().getOrThrowException();
    Assert.assertTrue(validationException.getFailures().isEmpty());
  }

  @Test
  public void test() throws IOException, AribaException, InterruptedException {
    AribaConnector connector = new AribaConnector(new AribaConnectorConfig(pluginConfig.getConnection().getClientId()
      , pluginConfig.getConnection().getClientSecret(), pluginConfig.getConnection().getBaseURL(),
                                                                           pluginConfig.getConnection().getRealm(),
                                                                           pluginConfig.getConnection().getSystemType()
                                                                          , pluginConfig.getConnection().getApiKey()));
    testTest(connector);
    testGenerateSpec(connector);
  }

  private void testGenerateSpec(AribaConnector connector) throws IOException, AribaException, InterruptedException {

    aribaServices = new AribaServices(pluginConfig.getConnection());
    URL url = null;
    InputStream inputStream = new ByteArrayInputStream(jsonNode.getBytes());
    new Expectations(AribaServices.class) {
      {
        aribaServices.fetchAribaResponse(url, anyString);
        result = response;
        minTimes = 0;

        response.getResponseBody();
        result = inputStream;
        minTimes = 0;

        response.getHttpStatusCode();
        result = 200;
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "testToken";
        minTimes = 0;
      }
    };
    AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();
    columnDetail.viewTemplateName("name2")
      .name("name").isPrimaryKey(false).type(ResourceConstants.OBJECT).size(0)
      .isCustomField(false).scale(0).precision(0)
      .childList(null);
    AribaColumnMetadata columnList = columnDetail.build();
    List<AribaColumnMetadata> columnDetails = new ArrayList<>();
    columnDetails.add(columnList);
    aribaServices = new AribaServices(pluginConfig.getConnection());
    new Expectations(AribaServices.class) {
      {
        aribaServices.getMetadata(anyString, anyString);
        result = columnDetails;
        minTimes = 0;
      }
    };

    ConnectorSpec connectorSpec = connector.generateSpec(new MockConnectorContext(new MockConnectorConfigurer()),
                                                         ConnectorSpecRequest.builder().setPath
                                                             (pluginConfig.getViewTemplateName())
                                                           .setConnection("${conn(connection-id)}").build());

    Schema schema = connectorSpec.getSchema();
    for (Schema.Field field : schema.getFields()) {
      Assert.assertNotNull(field.getSchema());
    }
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(1, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(AribaBatchSource.NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());

    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
  }
}
