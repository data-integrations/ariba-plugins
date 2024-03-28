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
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.exception.AribaRetryableException;
import io.cdap.plugin.ariba.source.metadata.AribaColumnMetadata;
import io.cdap.plugin.ariba.source.metadata.AribaResponseContainer;
import io.cdap.plugin.ariba.source.metadata.AribaSchemaGenerator;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import mockit.Expectations;
import mockit.Mocked;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Test case for AribaServices
 */
public class AribaServicesTest {

  @Mocked
  AribaResponseContainer response;
  String jsonNode = "{" +
    "  \"type\": \"object\"," +
    "  \"access_token\": \"jiuokiopu\"," +
    "  \"status\": \"completed\"," +
    "  \"totalNumOfPages\": \"1\"," +
    "  \"currentPageNum\": \"1\"," +
    "  \"message\": \"test\"," +
    "  \"properties\": {" +
    "    \"IsTestProject\": {" +
    "      \"title\": \"IsTestProject\"," +
    "      \"type\": [" +
    "        \"boolean\"," +
    "        \"null\"" +
    "      ]," +
    "      \"precision\": null," +
    "      \"scale\": null," +
    "      \"size\": null," +
    "      \"allowedValues\": null" +
    "    }," +
    "   \"Owner\": {" +
    "      \"type\": [" +
    "        \"object\"," +
    "        \"null\"" +
    "      ]," +
    "      \"properties\": {" +
    "        \"SourceSystem\": {" +
    "          \"title\": \"Owner.SourceSystem\"," +
    "          \"type\": [" +
    "            \"string\"," +
    "            \"null\"" +
    "          ]," +
    "          \"precision\": null," +
    "          \"scale\": null," +
    "          \"size\": 100," +
    "          \"allowedValues\": null" +
    "        }," +
    "        \"UserId\": {" +
    "          \"title\": \"Owner.UserId\"," +
    "          \"type\": [" +
    "            \"string\"," +
    "            \"null\"" +
    "          ]," +
    "          \"precision\": null," +
    "          \"scale\": null," +
    "          \"size\": 50," +
    "          \"allowedValues\": null" +
    "        }" +
    "      }" +
    "    }," +
    "     \"Organization\": {" +
    "      \"type\": [" +
    "        \"array\"," +
    "        \"null\"" +
    "      ]," +
    "      \"items\": [" +
    "        {" +
    "          \"type\": [" +
    "            \"object\"," +
    "            \"null\"" +
    "          ]," +
    "          \"properties\": {}" +
    "        }," +
    "         {" +
    "          \"type\": [" +
    "            \"array\"," +
    "            \"null\"" +
    "          ]," +
    "          " +
    "  \"items\": [" +
    "        {" +
    "          \"type\": [" +
    "            \"object\"," +
    "            \"null\"" +
    "          ]," +
    "          \"properties\": {}" +
    "        }," +
    "        {" +
    "          \"type\": [" +
    "            \"array\"," +
    "            \"null\"" +
    "          ]," +
    "          \"properties\": {}" +
    "        }" +
    "      ]" +
    "        }" +
    "        " +
    "      ]" +
    "    }" +
    "  }" +
    "}";
  private AribaServices aribaServices;
  private AribaPluginConfig pluginConfig;
  private AribaPluginConfig.Builder pluginConfigBuilder;

  @Before
  public void setUp() {
    pluginConfigBuilder = new AribaPluginConfig.Builder()
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
  public void testGetAccessToken() throws AribaException, IOException {
    String tokenUrl = String.format("https://%s", "https://api.au.cloud.ariba.com/v2/oauth/token");
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(tokenUrl))
      .newBuilder().addPathSegments("TOKEN_PATH");

    InputStream inputStream = new ByteArrayInputStream(jsonNode.getBytes(StandardCharsets.UTF_8));
    new Expectations(AribaServices.class) {
      {
        aribaServices.generateTokenURL();
        result = any;
        minTimes = 0;

        aribaServices.callAribaForToken((URL) any);
        result = response;
        minTimes = 0;

        response.getResponseBody();
        result = inputStream;
        minTimes = 0;

        response.getHttpStatusCode();
        result = 200;
      }
    };

    String accessToken = aribaServices.getAccessToken();
    Assert.assertNotNull(accessToken);
    Assert.assertEquals("jiuokiopu", accessToken);

  }

  @Test
  public void testGenerateTokenURL() {
    Assert.assertEquals("https://api.token.ariba.com/v2/oauth/token",
                        aribaServices.generateTokenURL().toString());
  }

  @Test
  public void testCallAribaForTokenForError() throws IOException {
    String tokenUrl = String.format("https://%s", "https://api.au.cloud.ariba.com/v2/oauth/token");
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(tokenUrl))
      .newBuilder().addPathSegments("TOKEN_PATH");
    new Expectations(AribaServices.class) {
      {
        aribaServices.httpAribaTokenCall(builder.build().url());
        result = new IOException();
        minTimes = 0;
      }
    };
    try {
      aribaServices.callAribaForToken(builder.build().url());
      Assert.fail("testCallAribaForTokenForError expected to fail with " +
                    "'Failed to call given Ariba service', but succeeded");
    } catch (AribaException aib) {
      Assert.assertEquals("Token Endpoint is incorrect.", aib.getMessage());
    }
  }

  @Test
  public void testBuildOutputSchema() throws AribaException, IOException, InterruptedException {
    AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();
    columnDetail.viewTemplateName("name2")
      .name("name").isPrimaryKey(false).type(ResourceConstants.OBJECT).size(0)
      .isCustomField(false).scale(0).precision(0)
      .childList(null);
    AribaColumnMetadata columnList = columnDetail.build();
    List<AribaColumnMetadata> columnDetails = new ArrayList<>();
    columnDetails.add(columnList);
    new Expectations(AribaServices.class) {
      {
        aribaServices.getMetadata(anyString, anyString);
        result = columnDetails;
        minTimes = 0;
      }
    };
    aribaServices.buildOutputSchema("token", "template");
    AribaSchemaGenerator aribaSchemaGenerator = new AribaSchemaGenerator(Collections.singletonList(columnList));
    Assert.assertEquals("RECORD", aribaSchemaGenerator.buildSchema().getType().toString());
  }

  @Test
  public void testGetMetadata() throws AribaException, IOException, InterruptedException {
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

    List<AribaColumnMetadata> aribaColumnMetadata = aribaServices.getMetadata("token",
                                                                              "template");
    Assert.assertEquals(2, aribaColumnMetadata.size());
  }

  @Test
  public void testGetMetadataWithError() throws AribaException, IOException, InterruptedException {
    URL url = null;
    AribaResponseContainer responseContainer = new AribaResponseContainer(404,
                                                                          "URL not forund", null);
    InputStream inputStream = new ByteArrayInputStream(jsonNode.getBytes());
    new Expectations(AribaServices.class) {
      {
        aribaServices.fetchAribaResponse(url, anyString);
        result = responseContainer;
        minTimes = 0;

        response.getResponseBody();
        result = inputStream;
        minTimes = 0;

        response.getHttpStatusCode();
        result = 402;
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "testToken";
        minTimes = 0;
      }
    };
    try {
      List<AribaColumnMetadata> aribaColumnMetadata = aribaServices.getMetadata("token",
                                                                                "template");
      Assert.fail("testGetMetadataWithError expected to fail, but succeeded");
    } catch (AribaException aib) {
      Assert.assertEquals("test", aib.getMessage());
    }
  }

  @Test
  public void testFetchAribaResponse() throws AribaException, IOException, InterruptedException {
    URL url = null;
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response response = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(200) // status code
      .message("")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      ))
      .build();
    new Expectations(AribaServices.class, ObjectMapper.class) {
      {
        aribaServices.httpAribaCall(url, anyString);
        result = response;
        minTimes = 0;

      }
    };
    aribaServices.fetchAribaResponse(url, "");
    Assert.assertEquals(200, response.code());
  }

  @Test
  public void testCreateJob() throws AribaException, IOException, InterruptedException {
    Request re = null;
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response res = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(200) // status code
      .message("")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      )).header("X-RateLimit-Remaining-Day", "10")
      .build();
    InputStream inputStream = new ByteArrayInputStream(jsonNode.getBytes());
    new Expectations(AribaServices.class) {
      {
        aribaServices.buildJobRequest((URL) any, pluginConfig, anyString);
        result = mockRequest;
        minTimes = 0;

        aribaServices.executeRequest(mockRequest);
        result = res;
        minTimes = 0;

        response.getResponseBody();
        result = inputStream;
        minTimes = 0;

        response.getHttpStatusCode();
        result = 200;
        minTimes = 0;

        aribaServices.checkUpdateFilter(anyString);
        result = false;
        minTimes = 0;

        aribaServices.isApiLimitExhausted(res);
        result = false;
        minTimes = 0;
      }
    };
    JsonNode jsonNode = aribaServices.createJob(pluginConfig, "page_Token", "template");
    Assert.assertEquals(7, jsonNode.size());
  }

  @Test
  public void testCreateJobWithDate() throws AribaException, IOException, InterruptedException {
    Request re = null;
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response res = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(200) // status code
      .message("")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      )).header("X-RateLimit-Remaining-Day", "10")
      .build();
    InputStream inputStream = new ByteArrayInputStream(jsonNode.getBytes());
    new Expectations(AribaServices.class) {
      {
        aribaServices.buildJobRequest((URL) any, pluginConfig, anyString);
        result = mockRequest;
        minTimes = 0;

        aribaServices.executeRequest(mockRequest);
        result = res;
        minTimes = 0;

        response.getResponseBody();
        result = inputStream;
        minTimes = 0;

        response.getHttpStatusCode();
        result = 200;
        minTimes = 0;

        aribaServices.checkUpdateFilter(anyString);
        result = true;
        minTimes = 0;

        aribaServices.isApiLimitExhausted(res);
        result = false;
        minTimes = 0;
      }
    };
    JsonNode jsonNode = aribaServices.createJob(pluginConfig, "page_Token", "template");
    Assert.assertEquals(7, jsonNode.size());
  }

  @Test
  public void testFetchData() {
    URL url = null;

    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    new Expectations(AribaServices.class) {
      {
        aribaServices.fetchZipFileData(url, anyString);
        result = mockRequest;
        minTimes = 0;

      }
    };
    try {
      aribaServices.fetchData("jobId", "fileName");
      Assert.fail("testFetchData expected to fail with 'Call failed to get access token', but succeeded");
    } catch (Exception e) {
      Assert.assertEquals("Token Endpoint is incorrect.", e.getMessage());
    }

  }

  @Test
  public void testFetchZipFileData() {
    HttpUrl.Builder zipUrl = aribaServices.zipBuilder("jobId", "fileName");
    Request request = aribaServices.fetchZipFileData(zipUrl.build().url(), "endpoint");
    Assert.assertEquals("https://openapi.ariba.com/api/analytics-reporting-jobresult/v1/" +
      "prod/jobs/jobId/files/fileName?realm=test-realm", request.url().toString());
  }

  @Test
  public void testZipBuilder() {
    HttpUrl.Builder zipUrl = aribaServices.zipBuilder("jobId", "fileName");
    Request request = aribaServices.fetchZipFileData(zipUrl.build().url(), "endpoint");
    Assert.assertEquals("https://openapi.ariba.com/api/analytics-reporting-jobresult/" +
        "v1/prod/jobs/jobId/files/fileName?realm=test-realm", request.url().toString());
  }

  @Test
  public void testFetchJobStatus() throws AribaException, IOException, InterruptedException {
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response res = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(200) // status code
      .message("")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      )).header("X-RateLimit-Remaining-Day", "10")
      .build();
    InputStream inputStream = new ByteArrayInputStream(jsonNode.getBytes());
    new Expectations(AribaServices.class) {
      {
        aribaServices.executeRequest(mockRequest);
        result = res;
        minTimes = 0;

        aribaServices.buildFetchRequest((URL) any, "access_token");
        result = mockRequest;
        minTimes = 0;

        response.getResponseBody();
        result = inputStream;
        minTimes = 0;

        response.getHttpStatusCode();
        result = 200;
        minTimes = 0;

        aribaServices.checkUpdateFilter(anyString);
        result = true;
        minTimes = 0;

        aribaServices.isApiLimitExhausted(res);
        result = false;
        minTimes = 0;
      }
    };
    JsonNode jsonNode = aribaServices.fetchJobStatus("access_token", "jobId");
    Assert.assertEquals(7, jsonNode.size());
  }

  @Test
  public void testIsApiLimitExhaustedForDay() throws InterruptedException {
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response response = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(401) // status code
      .message("")
      .header("X-RateLimit-Remaining-Day", "0")
      .header("RateLimit-Reset", "0")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      ))
      .build();
    try {
      aribaServices.isApiLimitExhausted(response);
      aribaServices.checkAndThrowException(response, true);
      Assert.fail("testIsApiLimitExhaustedForDay expected to fail with " +
                    "'API rate limit exceeded for the Day', but succeeded");
    } catch (AribaException e) {
      Assert.assertEquals("API rate limit exceeded for the Day, Please retry after 1 hours.", e.getMessage());
      Assert.assertEquals(ResourceConstants.LIMIT_EXCEED_ERROR_CODE, e.getErrorCode());
    } catch (AribaRetryableException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testIsApiLimitExhaustedForHour() {
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response response = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(401) // status code
      .message("")
      .header("X-RateLimit-Remaining-Day", "1")
      .header("X-RateLimit-Remaining-Hour", "0")
      .header("RateLimit-Reset", "0")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      ))
      .build();

    Assert.assertTrue(aribaServices.isApiLimitExhausted(response));
  }

  @Test
  public void testIsApiLimitExhaustedForMinute() {
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response response = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(401) // status code
      .message("")
      .header("X-RateLimit-Remaining-Day", "1")
      .header("X-RateLimit-Remaining-Hour", "1")
      .header("X-RateLimit-Remaining-Minute", "0")
      .header("X-RateLimit-Remaining-Second", "0")
      .header("RateLimit-Reset", "0")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      ))
      .build();

    Assert.assertTrue(aribaServices.isApiLimitExhausted(response));
  }

  @Test
  public void testIsApiLimitExhaustedForSecond() {
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response response = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(429) // status code
      .message("")
      .header("X-RateLimit-Remaining-Day", "1")
      .header("X-RateLimit-Remaining-Hour", "1")
      .header("X-RateLimit-Remaining-Minute", "1")
      .header("X-RateLimit-Remaining-Second", "0")
      .header("RateLimit-Reset", "0")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      ))
      .build();

    Assert.assertTrue(aribaServices.isApiLimitExhausted(response));
  }

  @Test
  public void checkUpdateFilter() throws AribaException, IOException, InterruptedException {
    URL url = null;
    InputStream inputStream = new ByteArrayInputStream(jsonNode.getBytes());
    new Expectations(AribaServices.class) {
      {
        aribaServices.fetchAribaResponse(url, anyString);
        result = response;
        minTimes = 0;

        aribaServices.getAccessToken();
        result = "access-token";
        minTimes = 0;

        response.getResponseBody();
        result = inputStream;
        minTimes = 0;

        response.getHttpStatusCode();
        result = 200;
        minTimes = 0;
      }
    };
    Assert.assertFalse(aribaServices.checkUpdateFilter("template"));

  }

  @Test
  public void testGetArraySchemaAsObject() throws AribaException, IOException, InterruptedException {
    Map<String, String> arrayDocumentName = new HashMap<>();
    arrayDocumentName.put("Suppliers", "SupplierDim");
    ObjectMapper mapper = new ObjectMapper();
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
    List<AribaColumnMetadata> aribaColumnMetadata = aribaServices.getArraySchemaAsObject(arrayDocumentName,
                                                                                         "template");
    Assert.assertEquals(1, aribaColumnMetadata.size());
  }

  @Test
  public void testBuildDataRequest() {
    String tokenUrl = String.format("https://%s", "api.au.cloud.ariba.com/v2/oauth/token");
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(tokenUrl))
      .newBuilder().addPathSegments("TOKEN_PATH");
    Request request = aribaServices.buildDataRequest(builder.build().url(), "access_token");
    Assert.assertEquals(request.url().toString(), "https://api.au.cloud.ariba.com/v2/oauth/token/TOKEN_PATH");
  }

  @Test
  public void testHttpAribaCall() throws AribaException, IOException, InterruptedException {
    Request mockRequest = new Request.Builder()
      .url("https://some-url.com")
      .build();
    Response response = new Response.Builder()
      .request(mockRequest)
      .protocol(Protocol.HTTP_2)
      .code(200) // status code
      .message("")
      .body(ResponseBody.create(
        MediaType.get("application/json; charset=utf-8"),
        "{}"
      ))
      .build();
    String tokenUrl = String.format("https://%s", "api.au.cloud.ariba.com/v2/oauth/token");
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(tokenUrl))
      .newBuilder().addPathSegments("TOKEN_PATH");
    new Expectations(AribaServices.class) {
      {
        aribaServices.buildDataRequest((URL) any, "access_token");
        result = mockRequest;
        minTimes = 0;

        aribaServices.executeRequest(mockRequest);
        result = response;
        minTimes = 0;
      }
    };
    Response returnedResponse = aribaServices.httpAribaCall(builder.build().url(), "access_token");
    Assert.assertEquals(200, returnedResponse.code());
  }

  @Test
  public void testFetchDataBuilder() {
    HttpUrl.Builder builder = aribaServices.fetchDataBuilder("jobId");
    Assert.assertEquals(
      "https://openapi.ariba.com/api/analytics-reporting-jobresult/v1/prod/jobs/jobId?realm=test-realm",
      builder.build().url().toString());
  }

  @Test
  public void testBuildFetchRequestr() {
    String tokenUrl = String.format("https://%s", "https://api.au.cloud.ariba.com/v2/oauth/token");
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(tokenUrl))
      .newBuilder().addPathSegments("TOKEN_PATH");
    Request request = aribaServices.buildFetchRequest(builder.build().url(), "access_token");
    Assert.assertEquals(request.url().toString(),
      "https://https//api.au.cloud.ariba.com/v2/oauth/token/TOKEN_PATH");
  }

  @Test
  public void testApiLimitExhaustedFlagsReset() {
    Response hourLimitExhaustedResponse = new Response.Builder()
      .request(new Request.Builder().url("https://some-url.com").build())
      .protocol(Protocol.HTTP_2)
      .code(429)
      .message("Hourly limit exceeded")
      .header("X-RateLimit-Remaining-Day", "1")
      .header("X-RateLimit-Remaining-Hour", "0")
      .body(ResponseBody.create(MediaType.parse("application/json"), "{}"))
      .build();

    Assert.assertTrue(aribaServices.isApiLimitExhausted(hourLimitExhaustedResponse));
    Assert.assertTrue(aribaServices.isHourLimitExhausted);

    Response minuteLimitExhaustedResponse = new Response.Builder()
      .request(new Request.Builder().url("https://some-url.com").build())
      .protocol(Protocol.HTTP_2)
      .code(429)
      .message("Minute limit exceeded")
      .header("X-RateLimit-Remaining-Day", "1")
      .header("X-RateLimit-Remaining-Hour", "1")
      .header("X-RateLimit-Remaining-Minute", "0")
      .body(ResponseBody.create(MediaType.parse("application/json"), "{}"))
      .build();

    Assert.assertTrue(aribaServices.isApiLimitExhausted(minuteLimitExhaustedResponse));
    Assert.assertTrue(aribaServices.isMinuteLimitExhausted);
    Assert.assertFalse(aribaServices.isHourLimitExhausted);
  }
}
