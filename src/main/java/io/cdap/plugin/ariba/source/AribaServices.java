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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.metadata.AribaColumnMetadata;
import io.cdap.plugin.ariba.source.metadata.AribaResponseContainer;
import io.cdap.plugin.ariba.source.metadata.AribaSchemaGenerator;
import io.cdap.plugin.ariba.source.metadata.proto.AribaMetaResponse;
import io.cdap.plugin.ariba.source.metadata.proto.ArrayFields;
import io.cdap.plugin.ariba.source.metadata.proto.ObjectFields;
import io.cdap.plugin.ariba.source.metadata.proto.Properties;
import io.cdap.plugin.ariba.source.metadata.proto.SimpleFields;
import io.cdap.plugin.ariba.source.util.AribaUtil;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

/**
 * This {@code AribaServices} contains all the Ariba relevant service call implementations
 * - check the correctness of the formed URL
 * - builds the Output Schema
 */
public class AribaServices {

  private static final String DOCUMENT_TYPE = "documentType";
  private static final String AUTHORIZATION = "Authorization";
  private static final String API_KEY = "apiKey";
  private static final String ACCEPT = "Accept";
  private static final String POST = "POST";
  private static final String VIEW_TEMPLATE_NAME = "viewTemplateName";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String VIEW_TEMPLATES = "viewTemplates";
  private static final String PRODUCT = "product";
  private static final String REALM = "realm";
  private static final String URL_PATTERN = "(api\\S*?).com";
  private static final String TOKEN_PATH_SEGMENT = "v2/oauth/token";
  private static final String TOKEN_GRANT_TYPE = "grant_type=client_credentials";
  private static final String ANALYTICS = "analytics";
  private static final String METADATA = "metadata";
  private static final String ACCESS_TOKEN = "access_token";
  private static final String JSON_SCHEMA = "jsonSchema";
  private static final String METADATA_PATH = "api/analytics-reporting-view/v1";
  private static final String TRUE = "true";
  private static final String JOB_PATH = "api/analytics-reporting-job/v1";
  private static final String JOBS = "jobs";
  private static final String JOB_RESULT_PATH = "api/analytics-reporting-jobresult/v1";
  private static final String APP_JSON = "application/json; charset=utf-8";
  private static final String FILTER_EXPRESSIONS = "filterExpressions";
  private static final String UPDATE_DATE = "updatedDate";
  private static final String RATE_LIMIT_DAY = "X-RateLimit-Remaining-Day";
  private static final String RATE_LIMIT_HOUR = "X-RateLimit-Remaining-Hour";
  private static final String RATE_LIMIT_MINUTE = "X-RateLimit-Remaining-Minute";
  private static final String RATE_LIMIT_SECOND = "X-RateLimit-Remaining-Second";
  private static final String SELECT_FIELDS = "selectFields";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String HTTPS_PATH = "https://%s";
  private static final String UTC = "UTC";
  private static final Logger LOG = LoggerFactory.getLogger(AribaServices.class);
  private static final int MAX_RETRIES = 5;
  private final AribaPluginConfig pluginConfig;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Gson gson = new Gson();
  private int availableLimit;
  private boolean isDayLimitExhausted;
  private boolean isHourLimitExhausted;
  private boolean isMinuteLimitExhausted;
  private boolean isSecondsLimitExhausted;

  public AribaServices(AribaPluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
  }

  /**
   * calls to get Object fields
   *
   * @param retMap retMap
   * @return List<AribaColumnMetadata>
   */
  private List<AribaColumnMetadata> getObjectFields(Map<String, ObjectFields> retMap) {
    List<AribaColumnMetadata> aribaColumnMetadata = new ArrayList<>();
    retMap.forEach((v1, v2) -> {
      AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();
      columnDetail.viewTemplateName(pluginConfig.getViewTemplateName())
        .name(v1).isPrimaryKey(false).type(ResourceConstants.OBJECT).size(0)
        .isCustomField(false).scale(0).precision(0)
        .childList(generateColumnValues(Properties.getNonObjectFields(v2.getProperties())));
      aribaColumnMetadata.add(columnDetail.build());
    });
    return aribaColumnMetadata;
  }

  /**
   * calls to get array fields
   *
   * @param retMap retMap
   * @return List<AribaColumnMetadata>
   * @throws AribaException
   * @throws IOException
   */
  private List<AribaColumnMetadata> getArrayFields(Map<String, ArrayFields> retMap)
    throws AribaException, IOException, InterruptedException {
    Map<String, String> arrayDocumentName = new HashMap<>();

    HttpUrl.Builder templateBuilder = metadataTemplateBuilder(true, null);
    AribaResponseContainer responseContainer = fetchAribaResponse(templateBuilder.build().url(), getAccessToken());
    InputStream responseStream = responseContainer.getResponseBody();
    JsonNode completeSchema = objectMapper.readTree(responseStream);

    for (String columnMetadata : retMap.keySet()) {
      if (completeSchema.get(SELECT_FIELDS) instanceof ArrayNode) {
        ArrayNode arrayNode = (ArrayNode) completeSchema.get(SELECT_FIELDS);
        for (JsonNode localNode : arrayNode) {
          if (String.format("%s.%s", columnMetadata, columnMetadata)
            .equalsIgnoreCase(localNode.get(NAME).asText())) {
            arrayDocumentName.put(columnMetadata, localNode.get(TYPE).asText());
          }
        }
      }

    }
    return getArraySchemaAsObject(arrayDocumentName);
  }

  /**
   * calls to get array schema
   *
   * @param arrayDocumentName array document name
   * @return List<AribaColumnMetadata>
   * @throws IOException
   * @throws AribaException
   */
  @VisibleForTesting
  List<AribaColumnMetadata> getArraySchemaAsObject(Map<String, String> arrayDocumentName)
    throws IOException, AribaException, InterruptedException {
    List<AribaColumnMetadata> aribaColumnMetadata = new ArrayList<>();

    for (Map.Entry<String, String> entry : arrayDocumentName.entrySet()) {
      HttpUrl.Builder templateBuilder = metadataTemplateBuilder(false, entry.getValue());
      AribaResponseContainer responseContainer = fetchAribaResponse(templateBuilder.build().url(), getAccessToken());
      InputStream responseStream = responseContainer.getResponseBody();
      JsonNode completeSchema = objectMapper.readTree(responseStream);
      AribaMetaResponse res = gson.fromJson(completeSchema.toString(), AribaMetaResponse.class);
      AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();
      columnDetail.viewTemplateName(pluginConfig.getViewTemplateName())
        .name(entry.getKey())
        .isPrimaryKey(false).type(ResourceConstants.ARRAY).size(0)
        .isCustomField(false).scale(0).precision(0);
      // Simple Fields
      List<AribaColumnMetadata> array = new ArrayList<>(generateColumnValues(
        Properties.getNonObjectFields(res.getProperties())));
      columnDetail.childList(array);
      aribaColumnMetadata.add(columnDetail.build());
    }

    return aribaColumnMetadata;
  }

  /**
   * Calls to check Ariba Connection.
   */
  public String getAccessToken() throws AribaException, IOException {
    LOG.trace("Initiating Ariba connection for access token");
    AribaResponseContainer responseContainer = callAribaForToken(generateTokenURL());
    try (InputStream responseStream = responseContainer.getResponseBody()) {
      if (responseContainer.getHttpStatusCode() != HttpURLConnection.HTTP_OK) {
        String errMsg = ResourceConstants.ERR_FETCHING_TOKEN.getMsgForKey();
        throw new AribaException(errMsg, responseContainer.getHttpStatusCode());
      }
      return objectMapper.readTree(responseStream).get(ACCESS_TOKEN).asText();
    }
  }

  /**
   * Construct metadata URL.
   *
   * @return URL.
   */
  @VisibleForTesting
  protected URL generateTokenURL() {
    String tokenUrl = String.format(HTTPS_PATH, fetchAuthURL());
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(tokenUrl))
      .newBuilder().addPathSegments(TOKEN_PATH_SEGMENT);
    return builder.build().url();
  }

  /**
   * Fetches Authentication auth url from base url
   * Base URL: http://openapi.au.cloud.ariba.com
   * Auth URL: https://api.au.cloud.ariba.com
   *
   * @return auth url string
   */
  private String fetchAuthURL() {
    Pattern urlPattern = Pattern.compile(URL_PATTERN);
    Matcher matcher = urlPattern.matcher(pluginConfig.getBaseURL());
    if (matcher.find()) {
      return matcher.group(0);
    }
    return pluginConfig.getBaseURL();
  }

  /**
   * Calls the Ariba for the given URL and returns the respective response.
   * Supported calls are:
   * - test Ariba connection
   *
   * @param endpoint type of URL
   * @return {@code AribaResponseContainer}
   */
  @VisibleForTesting
  protected AribaResponseContainer callAribaForToken(URL endpoint) throws AribaException {
    try {
      Response res = httpAribaTokenCall(endpoint);
      return tokenResponse(res);
    } catch (IOException ioe) {
      throw new AribaException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(),
                               ResourceConstants.DEFAULT_CODE);
    }
  }

  /**
   * Prepares the {@code AribaResponseContainer} from the given {@code Response}.
   *
   * @param res {@code Response}
   * @return {@code AribaResponseContainer}
   */
  private AribaResponseContainer tokenResponse(Response res) throws IOException {
    return AribaResponseContainer.builder()
      .httpStatusCode(res.code())
      .httpStatusMsg(res.message())
      .responseStream(res.body() != null ? Objects.requireNonNull(res.body()).bytes() : null)
      .build();
  }

  /**
   * Make an HTTP/S call to the given URL for token.
   *
   * @param endpoint Ariba OAuth URL
   * @return {@code Response}
   * @throws IOException any http client exceptions
   */
  @VisibleForTesting
  Response httpAribaTokenCall(URL endpoint) throws IOException {
    OkHttpClient enhancedOkHttpClient = getConfiguredClient().build();
    Request req = buildTokenRequest(endpoint);
    // No API limit on this call
    return enhancedOkHttpClient.newCall(req).execute();
  }

  /**
   * Builds the {@code OkHttpClient.Builder} with following optimized configuration parameters.
   * <p>
   * Connection Timeout in seconds: 300
   * Read Timeout in seconds: 300
   * Write Timeout in seconds: 300
   *
   * @return {@code OkHttpClient.Builder}
   */
  private OkHttpClient.Builder getConfiguredClient() {
    return new OkHttpClient.Builder()
      .readTimeout(300, TimeUnit.SECONDS)
      .writeTimeout(300, TimeUnit.SECONDS)
      .connectTimeout(300, TimeUnit.SECONDS);
  }

  /**
   * Prepares request for access token calls.
   * endpoint {@code endpoint}
   *
   * @return Request
   */
  private Request buildTokenRequest(URL endpoint) {
    String mediaType = MediaType.APPLICATION_FORM_URLENCODED;
    RequestBody body = RequestBody.create(TOKEN_GRANT_TYPE,
                                          okhttp3.MediaType.parse(mediaType));
    return new Request.Builder()
      .get().url(endpoint)
      .method(POST, body)
      .addHeader(AUTHORIZATION, String.format("Basic %s", getBase64EncodedValue()))
      .addHeader(CONTENT_TYPE, mediaType)
      .build();
  }

  /**
   * call to get Base64 encoded value
   *
   * @return base64 String
   */
  private String getBase64EncodedValue() {
    return Base64.getEncoder().encodeToString((pluginConfig.getClientId() +
      ":" + pluginConfig.getClientSecret()).getBytes());
  }

  /**
   * Prepares output schema based on the provided plugin config parameters.
   */
  public Schema buildOutputSchema(String accessToken) throws IOException, AribaException, InterruptedException {
    LOG.trace("Initiating Metadata Call To Ariba");
    List<AribaColumnMetadata> aribaColumnMetadataList = getMetadata(accessToken);
    AribaSchemaGenerator schemaGenerator = new AribaSchemaGenerator(aribaColumnMetadataList);
    return schemaGenerator.buildSchema();
  }

  /**
   * Fetch metadata from Ariba.
   *
   * @param accessToken {@code Access Token}
   */
  public List<AribaColumnMetadata> getMetadata(String accessToken) throws
    IOException, AribaException, InterruptedException {
    HttpUrl.Builder templateBuilder = metadataTemplateBuilder(false, null);
    AribaResponseContainer responseContainer = fetchAribaResponse(templateBuilder.build().url(), accessToken);
    InputStream responseStream = responseContainer.getResponseBody();
    JsonNode jsonNode = objectMapper.readTree(responseStream);
    if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_OK) {

      AribaMetaResponse res = gson.fromJson(jsonNode.toString(), AribaMetaResponse.class);
      // Simple Fields
      List<AribaColumnMetadata> columnDetails =
        generateColumnValues(Properties.getNonObjectFields(res.getProperties()));
      // Object Fields
      columnDetails.addAll(getObjectFields(Properties.getObjectFields(res.getProperties())));
      // Array Fields
      columnDetails.addAll(getArrayFields(Properties.getArrayFields(res.getProperties())));
      return columnDetails;
    }
    String errMsg = jsonNode.get(ResourceConstants.MESSAGE).asText() != null
      ? jsonNode.get(ResourceConstants.MESSAGE).asText() :
      ResourceConstants.ERR_NOT_FOUND.getMsgForKey();
    throw new AribaException(errMsg, responseContainer.getHttpStatusCode());
  }

  /**
   * @param aribaPluginConfig ariba plugin config
   * @return JsonNode
   */
  public JsonNode createJob(AribaPluginConfig aribaPluginConfig, @Nullable String pageToken) throws
    AribaException, IOException, InterruptedException {
    Request req = buildJobRequest(jobBuilder(pageToken).build().url(), aribaPluginConfig);
    int count = 0;
    Response response;
    do {
      response = executeRequest(req);
      if (response.code() == ResourceConstants.API_LIMIT_EXCEED) {
        checkAndThrowException(response);
        count++;
      }
    } while (response.code() == ResourceConstants.API_LIMIT_EXCEED && count <= MAX_RETRIES);

    AribaResponseContainer responseContainer = tokenResponse(response);
    InputStream responseStream = responseContainer.getResponseBody();
    if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_OK) {
      availableLimit = Integer.parseInt(Objects.requireNonNull(response.header(RATE_LIMIT_DAY)));
      LOG.info("Available limit from Create Job API: {}", availableLimit);
      return objectMapper.readTree(responseStream);
    }
    throw new AribaException(response.message(), response.code());
  }

  /**
   * @param accessToken access token
   * @param jobId       job Id
   * @return JsonNode
   */
  public JsonNode fetchJobStatus(String accessToken, String jobId)
    throws IOException, AribaException, InterruptedException {
    URL url = fetchDataBuilder(jobId).build().url();
    int count = 0;
    Request req = buildFetchRequest(url, accessToken);
    Response response = null;
    try {
      do {
        response = executeRequest(req);
        checkAndThrowException(response);
        count++;
      } while (response.code() == ResourceConstants.API_LIMIT_EXCEED && count <= MAX_RETRIES);
      AribaResponseContainer responseContainer = tokenResponse(response);

      if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_OK) {
        InputStream responseStream = responseContainer.getResponseBody();
        JsonNode responseNode = objectMapper.readTree(responseStream);
        String status = responseNode.get(ResourceConstants.STATUS).asText();
        if (status.equals(ResourceConstants.ERROR_MAX_REACHED) || status.equals(ResourceConstants.ERROR_INTERNAL) ||
          status.equals(ResourceConstants.ERROR_INVALID_DATE_RANGE)) {
          throw new AribaException(status);
        }
        verifyAvailableLimit(responseNode);
        return responseNode;
      }
      throw new AribaException(response.message(), response.code());
    } finally {
      response.close();
    }
  }

  /**
   * Check if available limit is less than required
   *
   * @param responseNode Ariba Response
   * @throws AribaException AribaException
   */
  private void verifyAvailableLimit(JsonNode responseNode) throws AribaException {
    if (responseNode.get(ResourceConstants.STATUS).asText().equals(ResourceConstants.COMPLETED) ||
      responseNode.get(ResourceConstants.STATUS).asText().equals(ResourceConstants.COMPLETED_ZERO_RECORDS)) {

      int remainingPages = responseNode.get(ResourceConstants.TOTAL_PAGES).asInt() -
        responseNode.get(ResourceConstants.CURRENT_PAGE).asInt();
      if (remainingPages > availableLimit) {
        LOG.info("Available limit: {} , Required Limit: {}",
                 availableLimit, remainingPages);
        throw new AribaException("Available API limit count is less then required",
                                 ResourceConstants.LIMIT_EXCEED_ERROR_CODE);
      }

    }
  }

  /**
   * Inject values in column fields
   *
   * @param jsonNode Response value in json format
   * @return list of AribaColumnMetadata.ColumnDetails
   */
  private List<AribaColumnMetadata> generateColumnValues(Map<String, SimpleFields> jsonNode) {
    List<AribaColumnMetadata> columnDetails = new ArrayList<>();
    jsonNode.forEach(
      (selectField, value) -> {
        if (selectField != null) {
          String name = value.getTitle().contains(ResourceConstants.DOT) ?
            value.getTitle().substring(value.getTitle()
                                         .lastIndexOf(ResourceConstants.DOT) + 1) : value.getTitle();
          String type = value.getType().get(0);
          int size = value.getSize() != null ? value.getSize() : 0;
          int precision = value.getPrecision() != null ? value.getPrecision() : 0;
          int scale = value.getScale() != null ? value.getScale() : 0;
          boolean isPrimaryKey = value.isPrimaryKey();

          AribaColumnMetadata columns =
            new AribaColumnMetadata(pluginConfig.getViewTemplateName(), name, type, size,
                                    false, precision, scale, isPrimaryKey, null);
          columnDetails.add(columns);
        }
      });
    return columnDetails;
  }

  /**
   * Calls the Ariba for the given URL and returns the respective response.
   * Supported calls are:
   * - fetching the Ariba metadata
   *
   * @param endpoint    type of URL
   * @param accessToken access token to make data calls
   * @return {@code AribaResponseContainer}
   */
  @VisibleForTesting
  AribaResponseContainer fetchAribaResponse(URL endpoint, String accessToken)
    throws IOException, AribaException, InterruptedException {
    Response res = httpAribaCall(endpoint, accessToken);
    return aribaResponse(res);
  }

  /**
   * Make an HTTP/S call to the given URL for token.
   *
   * @param endpoint Ariba OAuth URL
   * @return {@code Response}
   * @throws IOException any http client exceptions
   */
  @VisibleForTesting
  protected Response httpAribaCall(URL endpoint, String accessToken)
    throws IOException, AribaException, InterruptedException {
    Request req;
    if (accessToken != null) {
      req = buildDataRequest(endpoint, accessToken);
    } else {
      req = buildTokenRequest(endpoint);
    }
    return executeRequest(req);
  }

  /**
   * Prepares request for metadata and data calls.
   *
   * @return Request
   */
  @VisibleForTesting
  Request buildDataRequest(URL endpoint, String accessToken) {
    return new Request.Builder()
      .addHeader(AUTHORIZATION, getAuthenticationKey(accessToken))
      .addHeader(API_KEY, pluginConfig.getApiKey())
      .addHeader(ACCEPT, MediaType.APPLICATION_JSON)
      .get()
      .url(endpoint)
      .build();
  }

  /**
   * Create URL for job
   *
   * @return HttpUrl.Builder
   */
  private HttpUrl.Builder jobBuilder(String pageToken) {
    return Objects.requireNonNull(HttpUrl.parse(pluginConfig.getBaseURL()))
      .newBuilder()
      .addPathSegments(JOB_PATH)
      .addPathSegments(pluginConfig.getSystemType())
      .addPathSegments(JOBS)
      .addQueryParameter(REALM, pluginConfig.getRealm())
      .addQueryParameter(ResourceConstants.PAGE_TOKEN, pageToken);
  }

  /**
   * Builds the Authentication key with the help of access token.
   *
   * @return returns auth key
   */
  private String getAuthenticationKey(String accessToken) {
    return String.format("Bearer %s", accessToken);
  }

  /**
   * Prepares the {@code AribaResponseContainer} from the given {@code Response}.
   *
   * @param res {@code Response}
   * @return {@code AribaResponseContainer}
   */
  private AribaResponseContainer aribaResponse(Response res) throws IOException {
    return AribaResponseContainer.builder()
      .httpStatusCode(res.code())
      .httpStatusMsg(res.message())
      .responseStream(res.body() != null ? Objects.requireNonNull(res.body()).bytes() : null)
      .build();
  }

  /**
   * Build URL to make call to view Template in order to fetch metadata
   *
   * @return HttpUrl.Builder
   */
  private HttpUrl.Builder metadataTemplateBuilder(boolean isArraySchema, String documentName) {
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(pluginConfig.getBaseURL()))
      .newBuilder()
      .addPathSegments(METADATA_PATH)
      .addPathSegments(pluginConfig.getSystemType())
      .addPathSegments(METADATA)
      .addQueryParameter(PRODUCT, ANALYTICS)
      .addQueryParameter(REALM, pluginConfig.getRealm());
    if (!isArraySchema && documentName == null) {
      builder.addQueryParameter(JSON_SCHEMA, TRUE)
        .addQueryParameter(VIEW_TEMPLATE_NAME, pluginConfig.getViewTemplateName());
    } else {
      builder.addQueryParameter(VIEW_TEMPLATE_NAME, pluginConfig.getViewTemplateName());
    }
    if (!isArraySchema && documentName != null) {
      builder.removeAllQueryParameters(VIEW_TEMPLATE_NAME)
        .addQueryParameter(JSON_SCHEMA, TRUE)
        .addQueryParameter(DOCUMENT_TYPE, documentName);
    }
    return builder;
  }


  /**
   * @param jobId job Id
   * @return @return HttpUrl.Builder
   */
  @VisibleForTesting
  HttpUrl.Builder fetchDataBuilder(String jobId) {
    return Objects.requireNonNull(HttpUrl.parse(pluginConfig.getBaseURL()))
      .newBuilder()
      .addPathSegments(JOB_RESULT_PATH)
      .addPathSegments(pluginConfig.getSystemType())
      .addPathSegments(JOBS)
      .addPathSegments(jobId)
      .addQueryParameter(REALM, pluginConfig.getRealm());
  }

  /**
   * @param endPoint    end Point
   * @param accessToken access token
   * @return request
   */
  @VisibleForTesting
  protected Request buildFetchRequest(URL endPoint, String accessToken) {
    return new Request.Builder().get()
      .url(endPoint)
      .addHeader(AUTHORIZATION, getAuthenticationKey(accessToken))
      .addHeader(API_KEY, pluginConfig.getApiKey())
      .addHeader(ACCEPT, MediaType.APPLICATION_JSON)
      .build();
  }

  /**
   * @param req request
   * @return Response
   * @throws AribaException       AribaException
   * @throws InterruptedException InterruptedException
   * @throws IOException          IOException
   */
  public Response executeRequest(Request req) throws AribaException, InterruptedException, IOException {
    OkHttpClient enhancedOkHttpClient = getConfiguredClient().build();
    Response response = enhancedOkHttpClient.newCall(req).execute();
    if (response.code() != HttpURLConnection.HTTP_OK && AribaUtil.isNullOrEmpty(response.message())) {
      AribaResponseContainer responseContainer = aribaResponse(response);
      InputStream responseStream = responseContainer.getResponseBody();
      JsonNode jsonNode = objectMapper.readTree(responseStream);
      String errMsg = jsonNode.get(ResourceConstants.MESSAGE).asText() != null
        ? jsonNode.get(ResourceConstants.MESSAGE).asText() :
        ResourceConstants.ERR_NOT_FOUND.getMsgForKey();
      throw new AribaException(errMsg, response.code());
    }
    if (!isApiLimitExhausted(response)) {
      checkAndThrowException(response);
      return response;
    } else {
      return enhancedOkHttpClient.newCall(req).execute();
    }
  }

  /**
   * @param jobId Ariba Job Id
   * @return JsonNode
   */
  public JsonNode fetchData(String jobId, String fileName)
    throws IOException, InterruptedException, AribaException {

    OkHttpClient enhancedOkHttpClient = getConfiguredClient().build();
    HttpUrl.Builder zipUrl = zipBuilder(jobId, fileName);
    Response zipResponse;
    do {
      zipResponse = enhancedOkHttpClient
        .newCall(fetchZipFileData(zipUrl.build().url(), getAccessToken())).execute();
      if (zipResponse.code() == ResourceConstants.API_LIMIT_EXCEED) {
        checkAndThrowException(zipResponse);
      }
    } while (zipResponse.code() == ResourceConstants.API_LIMIT_EXCEED);

    LOG.info("Fetch Data Response Code is: {} for Job Id: {} , and File: {}"
      , zipResponse.code(), jobId, fileName);

    AribaResponseContainer responseContainer = tokenResponse(zipResponse);
    try (InputStream responseStream = responseContainer.getResponseBody();
         ZipInputStream zis = new ZipInputStream(Objects.requireNonNull(responseStream))) {
      zis.getNextEntry();
      return objectMapper.readTree(zis);
    }
  }

  /**
   * Checks for api limits
   * Job Submission: 1/second, 2/minute, 8/hour, 40/day
   * Fetch Schema: 1/second, 10/minute, 100/hour, 500/day
   * File Download: 2/second, 20/minute, 200/hour, 1000/day
   * Value of rate limit is return from API in headers section,
   * and we check for appropriate limits and sets the boolean value to true.
   *
   * @param response Request
   * @return boolean
   */
  public boolean isApiLimitExhausted(Response response) {
    if (response.code() != HttpURLConnection.HTTP_OK &&
      Integer.parseInt(Objects.requireNonNull(response.header(RATE_LIMIT_DAY))) < 1) {
      isDayLimitExhausted = true;
      return true;
    } else if (response.code() != HttpURLConnection.HTTP_OK &&
      Integer.parseInt(Objects.requireNonNull(response.header(RATE_LIMIT_HOUR))) < 1) {
      isHourLimitExhausted = true;
      return true;
    } else if (response.code() != HttpURLConnection.HTTP_OK &&
      Integer.parseInt(Objects.requireNonNull(response.header(RATE_LIMIT_MINUTE))) < 1) {
      isMinuteLimitExhausted = true;
      return true;
    } else if (response.code() != HttpURLConnection.HTTP_OK &&
      Integer.parseInt(Objects.requireNonNull(response.header(RATE_LIMIT_SECOND))) < 1) {
      isSecondsLimitExhausted = true;
      return true;
    }
    return false;
  }

  /**
   * Check for limit and status code than throws exception accordingly
   *
   * @param response response
   * @throws AribaException
   * @throws InterruptedException
   */
  @VisibleForTesting
  void checkAndThrowException(Response response) throws AribaException, InterruptedException {
    if (response.code() == HttpURLConnection.HTTP_BAD_REQUEST && !AribaUtil.isNullOrEmpty(response.message())) {
      throw new AribaException(response.message(), response.code());
    }
    boolean limitExhausted = isApiLimitExhausted(response);

    if (limitExhausted && isDayLimitExhausted) {
      int retryAfter =
        (Integer.parseInt(Objects.requireNonNull(response.header(ResourceConstants.RETRY_AFTER))) / 3600) + 1;
      LOG.info("API rate limit exceeded for the Day, Please retry after {} hours", retryAfter);
      throw new AribaException(ResourceConstants.ERR_API_LIMIT_EXCEED_FOR_DAY.getMsgForKey(retryAfter),
                               ResourceConstants.LIMIT_EXCEED_ERROR_CODE);
    } else if (limitExhausted && isHourLimitExhausted) {
      int retryAfter =
        (Integer.parseInt(Objects.requireNonNull(response.header(ResourceConstants.RETRY_AFTER))) / 60) + 1;
      LOG.info("API rate limit exceeded for the Hour, waiting for {} min", retryAfter);
      TimeUnit.MINUTES.sleep(retryAfter);
    } else if (limitExhausted && isMinuteLimitExhausted) {
      int retryAfter =
        (Integer.parseInt(Objects.requireNonNull(response.header(ResourceConstants.RETRY_AFTER))));
      LOG.debug("API rate limit exceeded for the Minute, waiting for {} Seconds", retryAfter);
      TimeUnit.SECONDS.sleep(retryAfter);
    } else if (limitExhausted && isSecondsLimitExhausted) {
      int retryAfter =
        (Integer.parseInt(Objects.requireNonNull(response.header(ResourceConstants.RETRY_AFTER))));
      LOG.debug("API rate limit exceeded for the Second, waiting for {} Seconds", retryAfter);
      TimeUnit.SECONDS.sleep(retryAfter);
    } else if (response.code() != HttpURLConnection.HTTP_OK) {
      throw new AribaException(response.message(), response.code());
    }

  }

  /**
   * @param endpoint
   * @param aribaPluginConfig
   * @return Request object
   */
  @VisibleForTesting
  protected Request buildJobRequest(URL endpoint, AribaPluginConfig aribaPluginConfig) throws
    AribaException, IOException, InterruptedException {
    LocalDateTime date = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    okhttp3.MediaType.parse(APP_JSON);
    String fromDate = aribaPluginConfig.getFromDate() != null ?
      aribaPluginConfig.getFromDate() : String.valueOf(date.minusYears(1)
                                                         .atZone((ZoneId.of(UTC))).withFixedOffsetZone());
    String toDate = aribaPluginConfig.getToDate() != null ?
      aribaPluginConfig.getToDate() : String.valueOf(date.atZone((ZoneId.of(UTC))).withFixedOffsetZone());
    RequestBody body;

    if (!checkUpdateFilter()) {
      body = RequestBody.create(okhttp3.MediaType.parse(APP_JSON),
                                String.format("{ " + "\"" + VIEW_TEMPLATE_NAME + "\": \"%s\",\"filters\": {\n" +
                                                "        \"createdDateFrom\": \"%s\",\n" +
                                                "        \"createdDateTo\": \"%s\"\n" +
                                                "    }}", aribaPluginConfig.getViewTemplateName(),
                                              fromDate, toDate));
    } else if (checkUpdateFilter() && aribaPluginConfig.getFromDate() != null) {
      body = RequestBody.create(okhttp3.MediaType.parse(APP_JSON),
                                String.format("{ " + "\"" + VIEW_TEMPLATE_NAME + "\": \"%s\",\"filters\": {\n" +
                                                "        \"updatedDateFrom\": \"%s\",\n" +
                                                "        \"updatedDateTo\": \"%s\"\n" +
                                                "    }}", aribaPluginConfig.getViewTemplateName(),
                                              fromDate, toDate));
    } else {
      body = RequestBody.create(okhttp3.MediaType.parse(APP_JSON),
                                String.format("{ " + "\"" + VIEW_TEMPLATE_NAME + "\": \"%s\"}",
                                              aribaPluginConfig.getViewTemplateName()));
    }
    return new Request.Builder()
      .addHeader(AUTHORIZATION, getAuthenticationKey(getAccessToken()))
      .addHeader(API_KEY, pluginConfig.getApiKey())
      .addHeader(ACCEPT, MediaType.APPLICATION_JSON)
      .method(POST, body)
      .url(endpoint)
      .build();
  }

  /**
   * calls for update filter
   *
   * @return @return boolean
   * @throws AribaException
   * @throws IOException
   */
  @VisibleForTesting
  protected boolean checkUpdateFilter() throws AribaException, IOException, InterruptedException {
    HttpUrl.Builder templateBuilder = filterTemplateBuilder();
    AribaResponseContainer responseContainer = fetchAribaResponse(templateBuilder.build().url(), getAccessToken());
    InputStream responseStream = responseContainer.getResponseBody();
    JsonNode jsonNode = objectMapper.readTree(responseStream);
    if (responseContainer.getHttpStatusCode() != HttpURLConnection.HTTP_OK ||
      jsonNode.get(FILTER_EXPRESSIONS) == null) {
      return false;
    } else {
      return !jsonNode.get(FILTER_EXPRESSIONS).isEmpty() && jsonNode.get(FILTER_EXPRESSIONS)
        .get(0).get(ResourceConstants.NAME).
        asText().contains(UPDATE_DATE);
    }
  }

  /**
   * @param jobId Job Id
   * @return @return HttpUrl.Builder
   */
  public HttpUrl.Builder zipBuilder(String jobId, String fileName) {
    return Objects.requireNonNull(HttpUrl.parse(pluginConfig.getBaseURL()))
      .newBuilder()
      .addPathSegments(JOB_RESULT_PATH)
      .addPathSegments(pluginConfig.getSystemType())
      .addPathSegments(JOBS)
      .addPathSegments(jobId)
      .addPathSegments(ResourceConstants.FILES)
      .addPathSegments(fileName)
      .addQueryParameter(REALM, pluginConfig.getRealm());
  }

  /**
   * Build URL to make call to view Template in order to fetch filter expressions
   *
   * @return HttpUrl.Builder
   */
  private HttpUrl.Builder filterTemplateBuilder() {
    return Objects.requireNonNull(HttpUrl.parse(pluginConfig.getBaseURL()))
      .newBuilder()
      .addPathSegments(METADATA_PATH)
      .addPathSegments(pluginConfig.getSystemType())
      .addPathSegments(VIEW_TEMPLATES)
      .addPathSegments(pluginConfig.getViewTemplateName())
      .addQueryParameter(PRODUCT, ANALYTICS)
      .addQueryParameter(REALM, pluginConfig.getRealm());
  }

  /**
   * @param endPoint
   * @param accessToken
   * @return request
   */
  @VisibleForTesting
  protected Request fetchZipFileData(URL endPoint, String accessToken) {
    return new Request.Builder().get()
      .url(endPoint)
      .addHeader(AUTHORIZATION, getAuthenticationKey(accessToken))
      .addHeader(API_KEY, pluginConfig.getApiKey())
      .build();
  }

}

