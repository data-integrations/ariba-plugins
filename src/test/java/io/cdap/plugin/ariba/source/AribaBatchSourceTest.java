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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.metadata.AribaSchemaGenerator;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import io.cdap.plugin.ariba.source.util.ResourceText;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.ResourceBundle;

import static java.util.Locale.ENGLISH;


public class AribaBatchSourceTest {

  @Tested
  private static AribaBatchSource aribaBatchSource;
  private MockPipelineConfigurer pipelineConfigurer;
  private AribaPluginConfig pluginConfig;
  private AribaServices aribaServices;
  private Schema schema;
  private AribaInputSplit aribaInputSplit;
  private ResourceConstants resourceConstants;

  @Mocked
  private BatchSourceContext context;

  @Mocked
  private FailureCollector failureCollector;

  @Before
  public void setUp() {
    pipelineConfigurer = new MockPipelineConfigurer(null);
    pluginConfig = new AribaPluginConfig("unit-test-ref-name",
                                         "https://openapi.au.cloud.ariba.com",
                                         "prod", "CloudsufiDSAPP-T",
                                         "SourcingProjectFactSystemView",
                                         "08ee0299-4849-42a4-8464-3abed75fc74e",
                                         "c3B5wvrEsjKucFGlGhKSWUDqDRGE2Wds",
                                         "xryi0757SU8pEyk7ePc7grc7vgDXdz8O",
                                         "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
  }

  @Test
  public void testConfigurePipelineWithInvalidBasicParam() {
    pluginConfig = new AribaPluginConfig("referenceName", "",
                                         "", "",
                                         "", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 6, failures.size());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("API Endpoint"),
                          failures.get(0).getMessage());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("System Type"),
                          failures.get(1).getMessage());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Realm Name"),
                          failures.get(2).getMessage());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("View Template Name"),
                          failures.get(3).getMessage());

    }
  }

  @Test
  public void testConfigurePipelineWithEmptyReferenceName() {
    pluginConfig = new AribaPluginConfig("", "url",
                                         "sysType", "realm",
                                         "template", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      //Reference name throws 2 failure, one from our method and another from CDAP Util class.
      Assert.assertEquals("Failures size does not match", 2, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Reference Name"),
                              failures.get(0).getMessage());
    }
  }


  @Test
  public void testConfigurePipelineWithEmptyClientIdAndClientSecret() {
    pluginConfig = new AribaPluginConfig("unit-test-ref-name",
                                         "https://openapi.au.cloud.ariba.com",
                                         "prod", "CloudsufiDSAPP-T",
                                         "SourcingProjectFactSystemView",
                                         "", "", "apiKey"
      , "2022-01-31T10:05:02Z",
                                         "2022-01-28T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("testConfigurePipelineWithEmptyClientIdAndClientSecret expected to fail, but succeeded");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      //ClientId and ClientSecret are blank
      Assert.assertEquals("Failures size does not match", 2, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba Client Id"),
                          failures.get(0).getMessage());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba Client Secret"),
                          failures.get(1).getMessage());
    }
  }

  @Test
  public void testValidateBasicParameters() {
    Assert.assertEquals("Template does not match", "SourcingProjectFactSystemView",
                        pluginConfig.getViewTemplateName());
    Assert.assertEquals("From date does not match", "2022-01-28T10:05:02Z",
                        pluginConfig.getFromDate());
    Assert.assertEquals("To date does not match", "2022-01-31T10:05:02Z", pluginConfig.getToDate());
  }

  @Test
  public void testValidateCredentialParameters() {
    pluginConfig = new AribaPluginConfig("referenceName", "baseUrl",
                                         "prod", "realm",
                                         "viewTemplateName", "",
                                         "", "",
                                         "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("testValidateCredentialParameters expected to fail, but succeeded");
    } catch (ValidationException s) {
      List<ValidationFailure> failures = s.getFailures();
      //clientId, clientSecret and apiKey are blank
      Assert.assertEquals("Failures size does not match", 3, failures.size());
    }
  }

  @Test
  public void testValidateAdvancedParametersError() {
    pluginConfig = new AribaPluginConfig("referenceName", "baseUrl",
                                         "prod", "realm",
                                         "viewTemplateName", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z",
                                         "2022-01-31T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("validateAdvancedParameters expected to fail, but succeeded");
    } catch (ValidationException s) {
      List<ValidationFailure> failures = s.getFailures();
      Assert.assertEquals("Failures size does not match, " +
                            "'CDF_ARIBA_01501 - Failed to call given Ariba service.'",
                          1, failures.size());
    }
  }

  @Test
  public void testValidateAdvancedParametersError2() {
    pluginConfig = new AribaPluginConfig("referenceName", "baseUrl",
                                         "prod", "realm",
                                         "viewTemplateName", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z", "");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("validateAdvancedParameters2 expected to fail, but succeeded");
    } catch (ValidationException s) {
      List<ValidationFailure> failures = s.getFailures();
      //Either provide both fromDate and toDate else provide none
      Assert.assertEquals("Failures size does not match", 1, failures.size());
    }
  }

  @Test
  public void testValidateAdvancedParametersError3() {
    pluginConfig = new AribaPluginConfig("referenceName", "baseUrl",
                                         "prod", "realm",
                                         "viewTemplateName", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z", "2021-01-28T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("validateAdvancedParameters3 expected to fail, but succeeded");
    } catch (ValidationException s) {
      List<ValidationFailure> failures = s.getFailures();
      //fromDate can not be greater then toDate
      Assert.assertEquals("Failures size does not match", 1, failures.size());

    }

  }

  @Test
  public void testValidateAdvancedParametersError4() {
    pluginConfig = new AribaPluginConfig("referenceName", "baseUrl",
                                         "prod", "realm",
                                         "viewTemplateName", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z", "2025-01-28T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("validateAdvancedParameters4 expected to fail, but succeeded");
    } catch (ValidationException s) {
      List<ValidationFailure> failures = s.getFailures();
      //Date can not be more than 1 year
      Assert.assertEquals("Failures size does not match", 1, failures.size());
    }
  }

  @Test
  public void testValidateAdvancedParametersError5() {
    pluginConfig = new AribaPluginConfig("referenceName", "baseUrl",
                                         "prod", "realm",
                                         "viewTemplateName", "clientId",
                                         "clientSecret", "apiKey",
                                         "fromDate", "toDate");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("validateAdvancedParameters5 expected to fail, but succeeded");
    } catch (ValidationException s) {
      List<ValidationFailure> failures = s.getFailures();
      //Invalid property of fromDate and toDate
      Assert.assertEquals("Failures size does not match", 2, failures.size());
    }
  }

  @Test
  public void testValidateAdvancedParametersError6() {
    pluginConfig = new AribaPluginConfig("referenceName", "baseUrl",
                                         "prod", "realm",
                                         "viewTemplateName", "clientId",
                                         "clientSecret", "apiKey",
                                         "20220128T10:05:02Z", "20230128T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("validateAdvancedParameters6 expected to fail, but succeeded");
    } catch (ValidationException s) {
      List<ValidationFailure> failures = s.getFailures();
      //Invalid value in fromDate & toDate
      Assert.assertEquals("Failures size does not match", 2, failures.size());
    }
  }

  @Test
  public void testIsSchemaBuildRequired() {
    pluginConfig = new AribaPluginConfig("referenceName",
                                         "https://stackoverflow.com/questions/17225948/" +
                                           "parsing-error-for-date-field{{{browser_user_agent}}}",
                                         "prod", "realm",
                                         "viewTemplateName", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z", "2023-01-28T10:05:02Z");
    Assert.assertTrue(pluginConfig.isSchemaBuildRequired());
  }

  @Test
  public void testConfigurePipelineSchemaNotNull() throws IOException, AribaException, InterruptedException {
    aribaServices = new AribaServices(pluginConfig.getConnection());
    new Expectations(AribaSchemaGenerator.class, AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = "testToken";
        minTimes = 0;

        aribaServices.buildOutputSchema(anyString, anyString);
        result = getPluginSchema();
        minTimes = 0;
      }
    };
    aribaBatchSource = new AribaBatchSource(pluginConfig);
    aribaBatchSource.configurePipeline(pipelineConfigurer);
    schema = pipelineConfigurer.getOutputSchema();
    Assert.assertNotNull("Output Schema generated is Null", schema);
  }

  @Test
  public void testConfigurePipelineForException() throws IOException, AribaException, InterruptedException {
    aribaServices = new AribaServices(pluginConfig.getConnection());
    new Expectations(AribaSchemaGenerator.class, AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = new InterruptedException();
        minTimes = 0;

        aribaServices.buildOutputSchema(anyString, anyString);
        result = getPluginSchema();
        minTimes = 0;
      }
    };
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("testConfigurePipelineForException expected to fail, but succeeded");
    } catch (ValidationException te) {
      List<ValidationFailure> failures = te.getFailures();
      //InterruptedException is thrown purposely from getAccessToken
      Assert.assertEquals("Failures size does not match", 1, failures.size());
    }
  }

  @Test
  public void testConfigurePipelineForAribaException() throws IOException, AribaException, InterruptedException {
    aribaServices = new AribaServices(pluginConfig.getConnection());
    new Expectations(AribaSchemaGenerator.class, AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = new AribaException("UnAuthorized", HttpURLConnection.HTTP_UNAUTHORIZED);
        minTimes = 0;

        aribaServices.buildOutputSchema(anyString, anyString);
        result = getPluginSchema();
        minTimes = 0;
      }
    };
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("testConfigurePipelineForAribaException expected to fail, but succeeded");
    } catch (ValidationException te) {
      Assert.assertEquals("Failures size does not match",
                          "Errors were encountered during validation. UnAuthorized ", te.getMessage());
    }
  }

  @Test
  public void testConfigurePipelineForAribaException1() throws IOException, AribaException, InterruptedException {
    aribaServices = new AribaServices(pluginConfig.getConnection());
    new Expectations(AribaSchemaGenerator.class, AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = new AribaException("testException", HttpURLConnection.HTTP_ACCEPTED);
        minTimes = 0;

        aribaServices.buildOutputSchema(anyString, anyString);
        result = getPluginSchema();
        minTimes = 0;
      }
    };
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("testConfigurePipelineForAribaException1 expected to fail, but succeeded");
    } catch (ValidationException te) {
      List<ValidationFailure> failures = te.getFailures();
      //AribaException is thrown when calling getAccessToken method
      Assert.assertEquals("Failures size does not match", 1, failures.size());
    }
  }

  @Test
  public void testConfigurePipelineForAribaException2() throws IOException, AribaException, InterruptedException {
    aribaServices = new AribaServices(pluginConfig.getConnection());
    new Expectations(AribaSchemaGenerator.class, AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = new AribaException("Bad Request", HttpURLConnection.HTTP_BAD_REQUEST);
        minTimes = 0;

        aribaServices.buildOutputSchema(anyString, anyString);
        result = getPluginSchema();
        minTimes = 0;
      }
    };
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("testConfigurePipelineForAribaException2 expected to fail, but succeeded");
    } catch (ValidationException te) {
      List<ValidationFailure> failures = te.getFailures();
      //AribaException is thrown when calling getAccessToken method
      Assert.assertEquals("Failures size does not match", 1, failures.size());
    }
  }

  @Test
  public void testConfigurePipelineForAribaException3() throws IOException, AribaException, InterruptedException {
    aribaServices = new AribaServices(pluginConfig.getConnection());
    new Expectations(AribaSchemaGenerator.class, AribaServices.class) {
      {
        aribaServices.getAccessToken();
        result = new AribaException("Not Found", HttpURLConnection.HTTP_NOT_FOUND);
        minTimes = 0;

        aribaServices.buildOutputSchema(anyString, anyString);
        result = getPluginSchema();
        minTimes = 0;
      }
    };
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("testConfigurePipelineForAribaException3 expected to fail, but succeeded");
    } catch (ValidationException te) {
      List<ValidationFailure> failures = te.getFailures();
      //AribaException is thrown when calling getAccessToken method
      Assert.assertEquals("Failures size does not match", 1, failures.size());
    }
  }

  @Test
  public void testConfigurePipelineConnectionException() {
    new Expectations(AribaPluginConfig.class) {
      {
        pluginConfig.isSchemaBuildRequired();
        result = true;
        minTimes = 0;
      }
    };
    aribaBatchSource = new AribaBatchSource(pluginConfig);
    Assert.assertTrue(pluginConfig.isSchemaBuildRequired());
  }

  @Test
  public void testConfigurePipelineForSchemaNotRequired() {
    new Expectations(AribaPluginConfig.class) {
      {
        pluginConfig.isSchemaBuildRequired();
        result = false;
        minTimes = 0;
      }
    };
    aribaBatchSource = new AribaBatchSource(pluginConfig);
    aribaBatchSource.configurePipeline(pipelineConfigurer);
    Assert.assertFalse(pluginConfig.isSchemaBuildRequired());
  }

  @Test
  public void testGetFileName() {
    AribaInputSplit aribaInputSplit1 = new AribaInputSplit();
    Assert.assertNull(aribaInputSplit1.getFileName());
    aribaInputSplit = new AribaInputSplit("fileName", "jobId");
    Assert.assertEquals("fileName", aribaInputSplit.getFileName());
  }

  @Test
  public void testGetJobId() {
    aribaInputSplit = new AribaInputSplit("fileName", "jobId");
    Assert.assertEquals("jobId", aribaInputSplit.getJobId());
  }

  @Test
  public void testGetLength() {
    aribaInputSplit = new AribaInputSplit("fileName", "jobId");
    Assert.assertEquals(0, aribaInputSplit.getLength());
  }

  @Test
  public void testGetLocation() {
    aribaInputSplit = new AribaInputSplit("fileName", "jobId");
    Assert.assertEquals("fileName", aribaInputSplit.getFileName());
    Assert.assertEquals("jobId", aribaInputSplit.getJobId());
  }

  @Test
  public void testResourceText() {
    ResourceBundle var = ResourceBundle.getBundle("i10n/AribaBatchSourceBundle", ENGLISH);
    Assert.assertEquals("Please check the 'Basic' parameter values.",
                        var.getString("err.resource.not.found"));
    Assert.assertEquals("Please check the 'Basic' parameter values.",
                        ResourceText.getString(ENGLISH, "err.resource.not.found"));
    Assert.assertEquals("Please check the 'Basic' parameter values.",
                        ResourceText.getString("err.resource.not.found"));
    Assert.assertEquals("Please check the Basic parameter values.",
                        ResourceText.getString(ENGLISH, "err.resource.not.found", null));

  }

  @Test
  public void testResourceConstant() {
    resourceConstants = ResourceConstants.ERR_ARIBA_SERVICE_FAILURE;
    Assert.assertEquals("CDF_ARIBA_01501", resourceConstants.getCode());
    Assert.assertEquals("CDF_ARIBA_01501 - {1}", resourceConstants.getMsgForKeyWithCode());
  }

  @Test
  public void testPrepareRun() throws Exception {

    new Expectations(AribaServices.class) {
      {
        context.getOutputSchema();
        result = getPluginSchema();
        minTimes = 0;

        context.getPipelineName();
        result = "test";
        minTimes = 0;

        context.getNamespace();
        result = "default";
        minTimes = 0;

      }
    };
    aribaBatchSource = new AribaBatchSource(pluginConfig);
    aribaBatchSource.prepareRun(context);
    schema = pipelineConfigurer.getOutputSchema();
    Assert.assertNull("Output Schema is generated null", schema);
  }

  @Test
  public void testPrepareRunForNullSchema() throws Exception {
    aribaServices = new AribaServices(pluginConfig.getConnection());
    aribaBatchSource = new AribaBatchSource(pluginConfig);
    new Expectations(AribaServices.class, AribaBatchSource.class) {
      {
        context.getOutputSchema();
        result = null;
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "access_token";
        minTimes = 0;

        aribaServices.buildOutputSchema(anyString, anyString);
        result = null;
        minTimes = 0;
      }
    };
    try {
      aribaBatchSource.prepareRun(context);
      Assert.fail("testPrepareRunForNullSchema expected to fail, but succeeded");
    } catch (IllegalArgumentException exception) {
      Assert.assertEquals("Failed to call given Ariba service.", exception.getMessage());
    }
  }


  private Schema getPluginSchema() throws IOException {
    String schemaString = "{\"type\":\"record\",\"name\":\"AribaColumnMetadata\",\"fields\":" +
      "[{\"name\":\"SourcingProjectFact\",\"type\":{\"type\":\"record\",\"name\":\"SourcingProjectFact_Record\"," +
      "\"fields\":[{\"name\":\"LoadCreateTime\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
      "{\"name\":\"LoadUpdateTime\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"ProjectId\"," +
      "\"type\":\"string\"},{\"name\":\"Description\",\"type\":\"string\"},{\"name\":\"AclId\",\"type\":\"int\"}," +
      "{\"name\":\"Duration\",\"type\":\"int\"},{\"name\":\"BeginDate\",\"type\":{\"type\":\"int\"," +
      "\"logicalType\":\"date\"}},{\"name\":\"DueDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
      "{\"name\":\"EndDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"Status\"," +
      "\"type\":\"string\"},{\"name\":\"State\",\"type\":\"string\"},{\"name\":\"OnTimeOrLate\"," +
      "\"type\":\"string\"},{\"name\":\"Owner\",\"type\":\"string\"},{\"name\":\"ProjectInfo\"," +
      "\"type\":\"string\"},{\"name\":\"DependsOnProject\",\"type\":\"string\"},{\"name\":\"ContainerProject\"," +
      "\"type\":\"string\"},{\"name\":\"Process\",\"type\":\"string\"},{\"name\":\"Commodity__Commodity\"," +
      "\"type\":\"string\"},{\"name\":\"Organization__Organization\",\"type\":\"string\"}," +
      "{\"name\":\"Region__Region\",\"type\":\"string\"},{\"name\":\"IsTestProject\",\"type\":\"boolean\"}," +
      "{\"name\":\"SourceSystem\",\"type\":\"string\"},{\"name\":\"ProcessStatus\",\"type\":\"string\"}," +
      "{\"name\":\"AllOwners__AllOwners\",\"type\":\"string\"},{\"name\":\"BaselineSpend\",\"type\":\"int\"}," +
      "{\"name\":\"ActualSaving\",\"type\":\"int\"},{\"name\":\"TargetSavingsPct\",\"type\":\"int\"}," +
      "{\"name\":\"ContractMonths\",\"type\":\"int\"},{\"name\":\"EventType\",\"type\":\"string\"}," +
      "{\"name\":\"ContractEffectiveDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
      "{\"name\":\"ResultsDescription\",\"type\":\"string\"},{\"name\":\"AwardJustification\",\"type\":\"string\"}," +
      "{\"name\":\"PlannedStartDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
      "{\"name\":\"PlannedEndDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
      "{\"name\":\"SourcingMechanism\",\"type\":\"string\"},{\"name\":\"ExecutionStrategy\",\"type\":\"string\"}," +
      "{\"name\":\"ProjectReason\",\"type\":\"string\"},{\"name\":\"PlannedEventType\",\"type\":\"string\"}," +
      "{\"name\":\"Origin\",\"type\":\"int\"},{\"name\":\"Suppliers__Suppliers\",\"type\":\"string\"}," +
      "{\"name\":\"TimeCreated\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
      "{\"name\":\"TimeUpdated\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}]}}]}";

    return Schema.parseJson(schemaString);
  }
}
