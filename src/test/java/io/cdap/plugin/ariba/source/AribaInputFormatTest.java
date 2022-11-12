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
import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test cases for AribaInputFormat
 */
public class AribaInputFormatTest {

  @Mocked
  AribaServices aribaServices;

  String createJobResponse1 = "{\n" +
    "  \"jobId\": \"f7d28f11-4a8c-4e03-8ed5-480248d0f33a1652441048867\",\n" +
    "  \"files\": [\n" +
    "    \"Fl34cxcar.zip\"\n" +
    "  ],\n" +
    "  \"status\": \"completed\",\n" +
    "  \"pageToken\": null\n" +
    "}";
  String createJobResponse = "{\n" +
    "  \"jobId\": \"82c87933-6a14-4ce6-89c6-976937608b961652441819369\"\n" +
    "}";
  @Tested
  private AribaInputFormat aribaInputFormat;

  @Test
  public void testCreateRecordReader() throws IOException, AribaException, InterruptedException {
    AribaPluginConfig pluginConfig = new AribaPluginConfig("unit-test-ref-name",
                                                           "https://openapi.au.cloud.ariba.com",
                                                           "prod",
                                                           "CloudsufiDSAPP-T",
                                                           "SourcingProjectFactSystemView",
                                                           "08ee0299-4849-42a4-8464-3abed75fc74e",
                                                           "c3B5wvrEsjKucFGlGhKSWUDqDRGE2Wds",
                                                           "xryi0757SU8pEyk7ePc7grc7vgDXdz8O",
                                                           "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    aribaInputFormat = new AribaInputFormat();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode createJob = objectMapper.readTree(createJobResponse);
    JsonNode jobData = objectMapper.readTree(createJobResponse1);
    new Expectations(AribaServices.class) {
      {
        aribaServices.createJob(pluginConfig, anyString, anyString);
        result = createJob;
        minTimes = 0;

        aribaServices.fetchJobStatus(anyString, anyString);
        result = jobData;
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "token";
        minTimes = 0;
      }
    };
    aribaInputFormat.createJob(pluginConfig, aribaServices, false, "kk");
    Assert.assertNotNull(jobData);
    Assert.assertEquals(4, jobData.size());
  }

  @Test
  public void testCreateJobThrowError() throws IOException, AribaException, InterruptedException {
    AribaPluginConfig pluginConfig = new AribaPluginConfig("unit-test-ref-name",
                                                           "https://openapi.au.cloud.ariba.com",
                                                           "prod",
                                                           "CloudsufiDSAPP-T",
                                                           "SourcingProjectFactSystemView",
                                                           "08ee0299-4849-42a4-8464-3abed75fc74e",
                                                           "c3B5wvrEsjKucFGlGhKSWUDqDRGE2Wds",
                                                           "xryi0757SU8pEyk7ePc7grc7vgDXdz8O",
                                                           "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    aribaInputFormat = new AribaInputFormat();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jobData = objectMapper.readTree(createJobResponse1);
    new Expectations(AribaServices.class) {
      {
        aribaServices.createJob(pluginConfig, anyString, anyString);
        result = new AribaException("limit exceed", 429);
        minTimes = 0;

        aribaServices.fetchJobStatus(anyString, anyString);
        result = jobData;
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "token";
        minTimes = 0;
      }
    };
    try {
      aribaInputFormat.createJob(pluginConfig, aribaServices, false, null);
      Assert.fail("testCreateJobThrowError expected to fail with limit exceed error, but succeeded");
    } catch (IOException io) {
      Assert.assertEquals(io.getMessage(), "limit exceed");
    }
  }

  @Test
  public void testCreateJobThrowError1() throws IOException, AribaException, InterruptedException {
    AribaPluginConfig pluginConfig = new AribaPluginConfig("unit-test-ref-name",
                                                           "https://openapi.au.cloud.ariba.com",
                                                           "prod",
                                                           "CloudsufiDSAPP-T",
                                                           "SourcingProjectFactSystemView",
                                                           "08ee0299-4849-42a4-8464-3abed75fc74e",
                                                           "c3B5wvrEsjKucFGlGhKSWUDqDRGE2Wds",
                                                           "xryi0757SU8pEyk7ePc7grc7vgDXdz8O",
                                                           "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    aribaInputFormat = new AribaInputFormat();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode crateJob = objectMapper.readTree(createJobResponse);
    new Expectations(AribaServices.class) {
      {
        aribaServices.createJob(pluginConfig, anyString, anyString);
        result = crateJob;
        minTimes = 0;

        aribaServices.fetchJobStatus(anyString, anyString);
        result = new AribaException("limit exceed", 429);
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "token";
        minTimes = 0;
      }
    };

    try {
      aribaInputFormat.createJob(pluginConfig, aribaServices, false, null);
      Assert.fail("testCreateJobThrowError1 expected to fail with limit exceed error, but succeeded");
    } catch (IOException io) {
      Assert.assertEquals(io.getMessage(), "limit exceed");
    }
  }

  @Test
  public void testCreateJobThrowError3() throws IOException, AribaException, InterruptedException {
    AribaPluginConfig pluginConfig = new AribaPluginConfig("unit-test-ref-name",
                                                           "https://openapi.au.cloud.ariba.com",
                                                           "prod",
                                                           "CloudsufiDSAPP-T",
                                                           "SourcingProjectFactSystemView",
                                                           "08ee0299-4849-42a4-8464-3abed75fc74e",
                                                           "c3B5wvrEsjKucFGlGhKSWUDqDRGE2Wds",
                                                           "xryi0757SU8pEyk7ePc7grc7vgDXdz8O",
                                                           "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    aribaInputFormat = new AribaInputFormat();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jobData = objectMapper.readTree(createJobResponse1);
    new Expectations(AribaServices.class) {
      {
        aribaServices.createJob(pluginConfig, anyString, anyString);
        result = new InterruptedException("InterruptedException");
        minTimes = 0;

        aribaServices.fetchJobStatus(anyString, anyString);
        result = jobData;
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "token";
        minTimes = 0;
      }
    };
    try {
      aribaInputFormat.createJob(pluginConfig, aribaServices, false, "kk");
      Assert.fail("testCreateJobThrowError3 expected to fail with InterruptedException, but succeeded");
    } catch (IOException io) {
      Assert.assertEquals(io.getMessage(), "InterruptedException");
    }
  }

  @Test
  public void testCreateJobThrowError4() throws IOException, AribaException, InterruptedException {
    AribaPluginConfig pluginConfig = new AribaPluginConfig("unit-test-ref-name",
                                                           "https://openapi.au.cloud.ariba.com",
                                                           "prod",
                                                           "CloudsufiDSAPP-T",
                                                           "SourcingProjectFactSystemView",
                                                           "08ee0299-4849-42a4-8464-3abed75fc74e",
                                                           "c3B5wvrEsjKucFGlGhKSWUDqDRGE2Wds",
                                                           "xryi0757SU8pEyk7ePc7grc7vgDXdz8O",
                                                           "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    aribaInputFormat = new AribaInputFormat();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode crateJob = objectMapper.readTree(createJobResponse);
    new Expectations(AribaServices.class) {
      {
        aribaServices.createJob(pluginConfig, anyString, anyString);
        result = crateJob;
        minTimes = 0;

        aribaServices.fetchJobStatus(anyString, anyString);
        result = new InterruptedException("InterruptedException");
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "token";
        minTimes = 0;
      }
    };
    try {
      aribaInputFormat.createJob(pluginConfig, aribaServices, false, "kk");
      Assert.fail("testCreateJobThrowError4 expected to fail with InterruptedException, but succeeded");
    } catch (IOException io) {
      Assert.assertEquals(io.getMessage(), "InterruptedException");
    }
  }


}
