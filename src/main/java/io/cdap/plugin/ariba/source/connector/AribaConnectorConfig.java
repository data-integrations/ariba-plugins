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

package io.cdap.plugin.ariba.source.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.ariba.source.AribaServices;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.metadata.AribaResponseContainer;
import io.cdap.plugin.ariba.source.util.AribaUtil;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;

/**
 * AribaConnectorConfig class
 */

public class AribaConnectorConfig extends PluginConfig {

  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String APIKEY = "apiKey";
  public static final String BASE_URL = "baseURL";
  public static final String REALM = "realm";
  public static final String TOKEN_URL = "tokenURL";
  private static final String COMMON_ACTION = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();
  private static final String METADATA_PATH = "api/analytics-reporting-view/v1";
  private static final String PATH_SEGMENT = "%s/viewTemplates";
  private static final String MESSAGE = "message";

  @Macro
  @Description("Ariba Client ID.")
  private final String clientId;

  @Macro
  @Description("Ariba Client Secret.")
  private final String clientSecret;

  @Macro
  @Description("Ariba Application Key.")
  private final String apiKey;

  @Macro
  @Description("Token Url to obtain the access token.")
  private final String tokenURL;

  @Macro
  @Description("Base Path of Ariba API.")
  private final String baseURL;

  @Macro
  @Description("Realm name from which data is to be extracted.")
  private final String realm;

  @Description("Type of system the Ariba instance is running on: Production or Sandbox.")
  private final String systemType;

  public AribaConnectorConfig(String clientId, String clientSecret, String apiKey, String baseURL, String realm,
                              String systemType, String tokenURL) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.apiKey = apiKey;
    this.baseURL = baseURL;
    this.realm = realm;
    this.systemType = systemType;
    this.tokenURL = tokenURL;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getApiKey() {
    return apiKey;
  }

  public String getBaseURL() {
    return baseURL;
  }

  public String getRealm() {
    return realm;
  }

  public String getSystemType() {
    return systemType;
  }

  public String getTokenURL() {
    return tokenURL;
  }

  /**
   * Validates the credentials parameters.
   *
   * @param failureCollector {@code FailureCollector}
   */
  public void validateCredentials(FailureCollector failureCollector) {

    if (AribaUtil.isNullOrEmpty(getClientId()) && !containsMacro(CLIENT_ID)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba Client Id");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(CLIENT_ID);
    }
    if (AribaUtil.isNullOrEmpty(getClientSecret()) && !containsMacro(CLIENT_SECRET)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba Client Secret");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(CLIENT_SECRET);
    }
    if (AribaUtil.isNullOrEmpty(getApiKey()) && !containsMacro(APIKEY)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba API Key");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(APIKEY);
    }
    if (AribaUtil.isNullOrEmpty(getBaseURL()) && !containsMacro(BASE_URL)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.API_END_POINT);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(BASE_URL);
    }
    if (AribaUtil.isNullOrEmpty(getRealm()) && !containsMacro(REALM)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.REALM_NAME);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(REALM);
    }
    if (AribaUtil.isNullOrEmpty(getTokenURL()) && !containsMacro(TOKEN_URL)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.TOKEN_URL);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(TOKEN_URL);
    }
  }

  public final void validateToken(FailureCollector collector) {
    AribaServices aribaServices = new AribaServices(this);
    try {
      String accessToken = aribaServices.getAccessToken();
      URL viewTemplatesURL = HttpUrl.parse(this.getBaseURL()).
        newBuilder()
        .addPathSegments(METADATA_PATH)
        .addPathSegments(String.format(PATH_SEGMENT, this.getSystemType()))
        .addQueryParameter(ResourceConstants.PRODUCT, ResourceConstants.ANALYTICS)
        .addQueryParameter(ResourceConstants.REALM, this.getRealm()).build().url();
      OkHttpClient enhancedOkHttpClient = new OkHttpClient();
      Request req = aribaServices.buildDataRequest(viewTemplatesURL, accessToken);
      Response response = enhancedOkHttpClient.newCall(req).execute();
      if (response.code() != HttpURLConnection.HTTP_OK) {
        getErrorFromResponse(aribaServices, response, collector);
      }
    } catch (UnknownHostException e) {
      collector.addFailure("API Endpoint is invalid", null);
    } catch (IOException | AribaException e) {
      collector.addFailure(e.getMessage(), null);
    }
  }

  private void getErrorFromResponse(AribaServices aribaServices, Response response, FailureCollector collector)
    throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    AribaResponseContainer responseContainer = aribaServices.tokenResponse(response);
    if (responseContainer.getResponseBody() != null) {
      InputStream responseStream = responseContainer.getResponseBody();
      String errResponse = objectMapper.readTree(responseStream).get(MESSAGE).asText();
      collector.addFailure(errResponse, null);
    } else {
      collector.addFailure("Credentials are incorrect", "Please check the credentials");
    }
  }
}
