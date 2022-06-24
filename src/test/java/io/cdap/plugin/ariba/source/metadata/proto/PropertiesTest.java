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
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.ariba.source.AribaServices;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.metadata.AribaColumnMetadata;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

public class PropertiesTest {

  private AribaPluginConfig pluginConfig;
  private final Gson gson = new Gson();
  private Properties properties;

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
  public void testGetObjectFieldCount()  {
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
}
