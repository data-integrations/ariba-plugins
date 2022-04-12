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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.metadata.AribaColumnMetadata;
import io.cdap.plugin.ariba.source.metadata.AribaResponseContainer;
import io.cdap.plugin.ariba.source.metadata.AribaSchemaGenerator;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String VIEW_TEMPLATE = "viewTemplates";
  private static final String PRODUCT = "product";
  private static final String REALM = "realm";
  private static final String URL_PATTERN = "(api\\S*?).com";
  private static final String TOKEN_PATH_SEGMENT = "v2/oauth/token";
  private static final String TOKEN_GRANT_TYPE = "grant_type=client_credentials";
  private static final String ANALYTICS_PATH_SEGMENT = "api/analytics-reporting-view/v1";
  private static final String ANALYTICS = "analytics";
  private static final String METADATA = "metadata";
  private static final String ACCESS_TOKEN = "access_token";

  private static final Logger LOG = LoggerFactory.getLogger(AribaServices.class);
  private final AribaPluginConfig pluginConfig;

  public AribaServices(AribaPluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
  }

  /**
   * Calls to check Ariba Connection.
   */
  public String getAccessToken() throws AribaException, IOException {
    LOG.trace("Initiating Ariba connection for access token");
    AribaResponseContainer responseContainer = callAribaForToken(generateTokenURL());
    try (InputStream responseStream = responseContainer.getResponseBody()) {
      ObjectMapper objectMapper = new ObjectMapper();
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
  private URL generateTokenURL() throws AribaException {
    String tokenUrl = fetchAuthURL();
    String newTokenUrl = (tokenUrl.startsWith("https://")) ? tokenUrl : "https://" + tokenUrl;
    HttpUrl.Builder builder = Objects.requireNonNull(HttpUrl.parse(newTokenUrl))
      .newBuilder().addPathSegments(TOKEN_PATH_SEGMENT);
    return builder.build().url();
  }

  /**
   * Fetches Authentication auth url from base url
   * Base URL: http://openapi.au.cloud.ariba.com
   * Auth URL: https://api.au.cloud.ariba.com
   * @return auth url string
   */
  private String fetchAuthURL() throws AribaException {
    Pattern urlPattern = Pattern.compile(URL_PATTERN);
    Matcher matcher = urlPattern.matcher(pluginConfig.getBaseURL());
    if (matcher.find()) {
      return matcher.group(0);
    }
    throw new AribaException(ResourceConstants.ERR_FETCHING_TOKEN_URL.getMsgForKey());
  }

  /**
   * Calls the Ariba for the given URL and returns the respective response.
   * Supported calls are:
   * - test Ariba connection
   *
   * @param endpoint type of URL
   * @return {@code AribaResponseContainer}
   */
  private AribaResponseContainer callAribaForToken(URL endpoint) throws AribaException {
    try {
      Response res = httpAribaTokenCall(endpoint);
      return tokenResponse(res);
    } catch (IOException ioe) {
      throw new AribaException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(),
                               ResourceConstants.DEFAULT_CODE, ioe);
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
  private Response httpAribaTokenCall(URL endpoint) throws IOException {
    OkHttpClient enhancedOkHttpClient = getConfiguredClient().build();
    Request req = buildTokenRequest(endpoint);
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

  private String getBase64EncodedValue() {
    return Base64.getEncoder().encodeToString((pluginConfig.getClientId() +
      ":" + pluginConfig.getClientSecret()).getBytes());
  }

  /**
   * Prepares output schema based on the provided plugin config parameters.
   */
  public Schema buildOutputSchema(String accessToken) throws IOException {
    LOG.trace("Initiating Metadata Call To Ariba");
    AribaColumnMetadata aribaColumnMetadataList = getMetadata(accessToken);
    AribaSchemaGenerator schemaGenerator = new AribaSchemaGenerator(aribaColumnMetadataList);
    return schemaGenerator.buildSchema();
  }

  /**
   * Fetch metadata from Ariba.
   */
  private AribaColumnMetadata getMetadata(String accessToken) throws IOException {
    AribaResponseContainer responseContainer = fetchAribaResponse(generateMetadataURL(accessToken), accessToken);
    try (InputStream responseStream = responseContainer.getResponseBody()) {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(responseStream);

      List<AribaColumnMetadata.ColumnDetails> columnDetails = generateColumnValues(jsonNode);
      return AribaColumnMetadata.builder()
        .documentType(jsonNode.get(DOCUMENT_TYPE).asText())
        .columnDetails(columnDetails).build();
    }
  }

  /**
   * Inject values in column fields
   *
   * @param jsonNode Response value in json format
   * @return list of AribaColumnMetadata.ColumnDetails
   */
  private List<AribaColumnMetadata.ColumnDetails> generateColumnValues(JsonNode jsonNode) {
    List<AribaColumnMetadata.ColumnDetails> columnDetails = new ArrayList<>();
    JsonNode selectFields = jsonNode.get(AribaColumnMetadata.SELECT_FIELDS);

    if (selectFields.isArray()) {
      for (JsonNode selectField : selectFields) {
        String name = selectField.get(AribaColumnMetadata.NAME).asText();
        String type = selectField.get(AribaColumnMetadata.TYPE).asText();
        int size = selectField.get(AribaColumnMetadata.SIZE).isEmpty() ? 0 :
          Integer.parseInt(selectField.get(AribaColumnMetadata.SIZE).asText());
        String allowedValues = selectField.get(AribaColumnMetadata.ALLOWED_VALUES).isEmpty() ? null :
          selectField.get(AribaColumnMetadata.ALLOWED_VALUES).asText();
        boolean isCustomField = Boolean.parseBoolean(selectField.get(AribaColumnMetadata.IS_CUSTOM_FIELD).asText());
        int precision = selectField.get(AribaColumnMetadata.SIZE).isEmpty() ? 0 :
          Integer.parseInt(selectField.get(AribaColumnMetadata.PRECISION).asText());
        int scale = selectField.get(AribaColumnMetadata.SIZE).isEmpty() ? 0 :
          Integer.parseInt(selectField.get(AribaColumnMetadata.SCALE).asText());

        AribaColumnMetadata.ColumnDetails columns =
          new AribaColumnMetadata.ColumnDetails(name, type, size, allowedValues, isCustomField, precision, scale);
        columnDetails.add(columns);
      }
    }
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
  private AribaResponseContainer fetchAribaResponse(URL endpoint, String accessToken) throws IOException {
    try {
      Response res = httpAribaDataCall(endpoint, accessToken);
      return aribaResponse(res);
    } catch (IOException ioe) {
      throw new IOException();
    }
  }

  /**
   * Make an HTTP/S call to the given URL for token.
   *
   * @param endpoint Ariba OAuth URL
   * @return {@code Response}
   * @throws IOException any http client exceptions
   */
  private Response httpAribaDataCall(URL endpoint, String accessToken) throws IOException {
    OkHttpClient enhancedOkHttpClient = getConfiguredClient().build();
    Request req = buildDataRequest(endpoint, accessToken);
    return enhancedOkHttpClient.newCall(req).execute();
  }

  /**
   * Prepares request for metadata and data calls.
   *
   * @return Request
   */
  private Request buildDataRequest(URL endpoint, String accessToken) {
    return new Request.Builder()
      .addHeader(AUTHORIZATION, getAuthenticationKey(accessToken))
      .addHeader(API_KEY, pluginConfig.getApiKey())
      .addHeader(ACCEPT, MediaType.APPLICATION_JSON)
      .get()
      .url(endpoint)
      .build();
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
   * Construct metadata URL.
   *
   * @return URL.
   */
  private URL generateMetadataURL(String accessToken) throws IOException {
    HttpUrl.Builder templateBuilder = templateBuilder();
    AribaResponseContainer responseContainer = fetchAribaResponse(templateBuilder.build().url(), accessToken);
    try (InputStream responseStream = responseContainer.getResponseBody()) {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(responseStream);

      HttpUrl.Builder builder = metaBuilder(jsonNode.get(DOCUMENT_TYPE).asText());
      return builder.build().url();
    }
  }

  /**
   * Build URL to make call to view Template in order to fetch document type
   *
   * @return HttpUrl.Builder
   */
  private HttpUrl.Builder templateBuilder() {
    return Objects.requireNonNull(HttpUrl.parse(pluginConfig.getBaseURL()))
      .newBuilder()
      .addPathSegments(ANALYTICS_PATH_SEGMENT)
      .addPathSegments(pluginConfig.getSystemType())
      .addPathSegments(VIEW_TEMPLATE)
      .addPathSegments(pluginConfig.getViewTemplateName())
      .addQueryParameter(PRODUCT, ANALYTICS)
      .addQueryParameter(REALM, pluginConfig.getRealm());
  }

  /**
   * Create metadata url with the help of documentType
   *
   * @return HttpUrl.Builder
   */
  private HttpUrl.Builder metaBuilder(String documentType) {
    return Objects.requireNonNull(HttpUrl.parse(pluginConfig.getBaseURL()))
      .newBuilder()
      .addPathSegments(ANALYTICS_PATH_SEGMENT)
      .addPathSegments(pluginConfig.getSystemType())
      .addPathSegments(METADATA)
      .addQueryParameter(DOCUMENT_TYPE, documentType)
      .addQueryParameter(REALM, pluginConfig.getRealm());
  }
}

