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

package io.cdap.plugin.ariba.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Test cases for AribaRecordReader
 */
public class AribaRecordReaderTest {

  private AribaPluginConfig pluginConfig;
  private AribaServices aribaServices;
  private MockPipelineConfigurer pipelineConfigurer;
  private static ObjectMapper mapper = new ObjectMapper();

  @Before
  public void setUp() {
    pipelineConfigurer = new MockPipelineConfigurer(null);
    AribaPluginConfig.Builder pluginConfigBuilder = new AribaPluginConfig.Builder()
      .referenceName("unit-test-ref-name")
      .baseURL("https://openapi.ariba.com")
      .systemType("prod")
      .realm("test-realm")
      .viewTemplateName("SourcingProjectFactSystemView")
      .clientId("client-id")
      .clientSecret("client-secret")
      .apiKey("api-key")
      .tokenURL("https://api.token.ariba.com")
      .fromDate("2022-01-28T10:05:02Z")
      .toDate("2022-01-31T10:05:02Z")
      .initialRetryDuration(AribaPluginConfig.DEFAULT_INITIAL_RETRY_DURATION_SECONDS)
      .maxRetryDuration(AribaPluginConfig.DEFAULT_MAX_RETRY_DURATION_SECONDS)
      .retryMultiplier(AribaPluginConfig.DEFAULT_RETRY_MULTIPLIER)
      .maxRetryCount(AribaPluginConfig.DEFAULT_MAX_RETRY_COUNT);
    pluginConfig = pluginConfigBuilder.build();

    aribaServices = new AribaServices(pluginConfig.getConnection(),
      pluginConfig.getMaxRetryCount(),
      pluginConfig.getInitialRetryDuration(),
      pluginConfig.getMaxRetryDuration(),
      pluginConfig.getRetryMultiplier(), false);
  }

  @Test
  public void testInitialize() throws IOException, AribaException, InterruptedException {
    AribaRecordReader aribaRecordReader = new AribaRecordReader(aribaServices, getPluginSchema(), pluginConfig);
    AribaInputSplit aribaInputSplit = new AribaInputSplit("sourceView.zip", "3343ddsfsg3434");
    AribaStructuredTransformer aribaStructuredTransformer = new AribaStructuredTransformer();
    JsonNode nodeRecord = mapper.readTree(rowData);
    StructuredRecord structuredRecord = aribaStructuredTransformer.readFields(nodeRecord, getPluginSchema());
    new Expectations(AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = "testToken";
        minTimes = 0;

        aribaServices.fetchData(anyString, anyString);
        result = nodeRecord;
        minTimes = 0;
      }
    };
    aribaRecordReader.initialize(aribaInputSplit, null);
    aribaRecordReader.nextKeyValue();
    String progress = String.valueOf(aribaRecordReader.getProgress());
    aribaRecordReader.close();
    Assert.assertEquals("WS13213262", structuredRecord.get("ProjectId"));
    Assert.assertTrue(aribaRecordReader.nextKeyValue());
    Assert.assertEquals("1.0", progress);
  }

  @Test
  public void testNextKeyValueFalse(@Mocked AribaInputSplit aribaInputSplit,
                                    @Mocked JsonNode node) throws IOException, AribaException, InterruptedException {
    AribaRecordReader aribaRecordReader = new AribaRecordReader(aribaServices, pipelineConfigurer.getOutputSchema(),
                                                                pluginConfig);
    new Expectations(AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = "testToken";
        minTimes = 0;

        aribaServices.fetchData(anyString, anyString);
        result = node;
        minTimes = 0;
      }
    };
    aribaRecordReader.initialize(aribaInputSplit, null);
    Assert.assertFalse(aribaRecordReader.nextKeyValue());
  }

  @Test
  public void testReadFields() throws IOException {
    AribaStructuredTransformer aribaStructuredTransformer = new AribaStructuredTransformer();
    JsonNode nodeRecord = mapper.readTree(rowData);
    StructuredRecord structuredRecord = aribaStructuredTransformer.readFields(nodeRecord, getPluginSchema());
    Assert.assertEquals("IsTestProject", structuredRecord.getSchema().getFields().get(0).getName());
  }

  @Test(expected = IOException.class)
  public void testInitializeError() throws IOException, InterruptedException, AribaException {
    AribaRecordReader aribaRecordReader = new AribaRecordReader(aribaServices, pipelineConfigurer.getOutputSchema(),
                                                                pluginConfig);
    AribaInputSplit aribaInputSplit = new AribaInputSplit("sourceView.zip", "3343ddsfsg3434");
    new Expectations(AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = "testToken";
        minTimes = 0;

        aribaServices.fetchData(anyString, anyString);
        result = new InterruptedException();
        minTimes = 0;
      }
    };
    aribaRecordReader.initialize(aribaInputSplit, null);
    Assert.fail("testInitializeError expected to fail, but succeeded");
  }


  public static String rowData = "{\"Origin\":null,\"IsTestProject\":false,\"Owner\":{\"SourceSystem\":\"ASM\"," +
    "\"UserId\":\"puser1\"," +
    "\"PasswordAdapter\":\"PasswordAdapter1\"},\"Description\":\"test sourcing RFP project\",\"Organization\":" +
    "[{\"Organization\":{\"OrganizationId\":\"\"}}],\"OnTimeOrLate\":\"On Time\",\"ProcessStatus\":\"A\"," +
    "\"EventType\":{\"EventType\":\"2\"},\"TargetSavingsPct\":0.0,\"DependsOnProject\":{\"ProjectId\":\"\"" +
    ",\"SourceSystem\":\"ASM\"},\"Process\":{\"SourceSystem\":\"ASM\",\"BaseProcessId\":\"\"},\"ContainerProject\"" +
    ":{\"ProjectId\":\"\",\"SourceSystem\":\"\"},\"AllOwners\":[{\"AllOwners\":{\"SourceSystem\":\"ASM\",\"UserId\"" +
    ":\"puser1\",\"PasswordAdapter\":\"PasswordAdapter1\"}}],\"PlannedStartDate\":{\"Day\":\"1970-01-01T00:00:00Z\"" +
    "},\"ContractEffectiveDate\":{\"Day\":\"1970-01-01T00:00:00Z\"},\"Suppliers\":[{\"Suppliers\":{\"SourceSystem\"" +
    ":\"ASM\",\"SupplierId\":\"\",\"SupplierLocationId\":\"\"}}],\"AwardJustification\":null,\"Commodity\":" +
    "[{\"Commodity\":{\"CommodityId\":\"\",\"SourceCommodityDomain\":\"unspsc\"}}],\"ContractMonths\":0.0,\"" +
    "Currency\":\"USD\",\"ProjectInfo\":{\"ProjectId\":\"WS13213262\",\"SourceSystem\":\"ASM\"},\"DueDate\":" +
    "{\"Day\":\"1970-01-01T00:00:00Z\"},\"ExecutionStrategy\":null,\"Status\":\"\",\"AclId\":49,\"" +
    "LoadUpdateTime\":\"2022-03-15T11:24:20Z\",\"ProjectId\":\"WS13213262\",\"Duration\":6.286053,\"" +
    "ProjectReason\":null,\"BaselineSpend\":\"1000000000000000000\",\"EndDate\":{\"Day\":\"2022-03-14T00:00:00Z\"},\"" +
    "ResultsDescription\":\"\",\"SourcingMechanism\":null,\"BeginDate\":{\"Day\":\"2022-03-09T00:00:00Z\"}," +
    "\"LoadCreateTime\":\"2022-03-09T10:23:06Z\",\"ActualSaving\":0.0,\"State\":\"Completed\",\"PlannedEndDate" +
    "\":{\"Day\":\"1970-01-01T00:00:00Z\"},\"Region\":[{\"Region\":{\"RegionId\":\"USA\"}}],\"PlannedEventType" +
    "\":{\"EventType\":\"\"},\"SourceSystem\":{\"SourceSystemId\":\"ASM\"}}";

  public static Schema getPluginSchema() throws IOException {
    String schemaString = "{" +
      "   \"type\":\"record\"," +
      "   \"name\":\"AribaColumnMetadata\"," +
      "   \"fields\":[" +
      "      {" +
      "         \"name\":\"IsTestProject\"," +
      "         \"type\":[" +
      "            \"boolean\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"Origin\"," +
      "         \"type\":[" +
      "            \"double\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"Status\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"Description\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"OnTimeOrLate\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"ProcessStatus\"," +
      "         \"type\":[" +
      "            \"bytes\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"TargetSavingsPct\"," +
      "         \"type\":[" +
      "            \"double\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"LoadUpdateTime\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"AclId\"," +
      "         \"type\":[" +
      "            \"int\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"ProjectId\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"Duration\"," +
      "         \"type\":[" +
      "            \"double\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"ProjectReason\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"BaselineSpend\"," +
      "         \"type\":[" +
      "            \"long\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"ResultsDescription\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"SourcingMechanism\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"AwardJustification\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"LoadCreateTime\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"ContractMonths\"," +
      "         \"type\":[" +
      "            \"double\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"ActualSaving\"," +
      "         \"type\":[" +
      "            \"double\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"State\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"Currency\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"ExecutionStrategy\"," +
      "         \"type\":[" +
      "            \"string\"," +
      "            \"null\"" +
      "         ]" +
      "      }," +
      "      {" +
      "         \"name\":\"Owner\"," +
      "         \"type\":[" +
      "            {" +
      "               \"type\":\"record\"," +
      "               \"name\":\"Owner\"," +
      "               \"fields\":[" +
      "                  {" +
      "                     \"name\":\"UserId\"," +
      "                     \"type\":[" +
      "                        \"string\"," +
      "                        \"null\"" +
      "                     ]" +
      "                  }," +
      "                  {" +
      "                     \"name\":\"SourceSystem\"," +
      "                     \"type\":[" +
      "                        \"string\"," +
      "                        \"null\"" +
      "                     ]" +
      "                  }" +
      "               ]" +
      "            }," +
      "            \"null\"" +
      "         ]" +
      "      }" +
      "   ]" +
      "}";

    return Schema.parseJson(schemaString);
  }

}
