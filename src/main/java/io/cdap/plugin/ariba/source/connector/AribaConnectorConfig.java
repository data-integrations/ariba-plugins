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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.ariba.source.AribaServices;
import io.cdap.plugin.ariba.source.exception.AribaException;
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
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import javax.ws.rs.core.MediaType;

/**
 * AribaConnectorConfig class
 */

public class AribaConnectorConfig extends PluginConfig {

  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String APIKEY = "apiKey";
  public static final String BASE_URL = "baseURL";
  public static final String REALM = "realm";
  private static final String TOKEN_GRANT_TYPE = "grant_type=client_credentials";
  private static final String POST = "POST";
  private static final String AUTHORIZATION = "Authorization";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String COMMON_ACTION = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();
  private static final Logger LOG = LoggerFactory.getLogger(AribaConnectorConfig.class);

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
  @Description("Base Path of Ariba API.")
  private final String baseURL;

  @Macro
  @Description("Realm name from which data is to be extracted.")
  private final String realm;

  @Description("Type of system the Ariba instance is running on: Production or Sandbox.")
  private final String systemType;

  public AribaConnectorConfig(String clientId, String clientSecret, String apiKey, String baseURL, String realm,
                              String systemType) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.apiKey = apiKey;
    this.baseURL = baseURL;
    this.realm = realm;
    this.systemType = systemType;
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
  }

  public final void validateToken(FailureCollector collector) {
    String tokenUrl = "https://api.au.cloud.ariba.com/v2/oauth/token";
    HttpUrl.Builder builder = HttpUrl.parse(tokenUrl).newBuilder();
    URL url = builder.build().url();
    try {
      if (httpAribaTokenCall(url).code() != HttpURLConnection.HTTP_OK) {
        collector.addFailure("Credentials are incorrect", "Please check the credentials");
      }
    } catch (IOException e) {
      LOG.error(" Unable to validate token", e);
    }
  }

  private Response httpAribaTokenCall(URL endpoint) throws IOException {
    OkHttpClient enhancedOkHttpClient = new OkHttpClient();
    Request req = buildTokenRequest(endpoint);
    // No API limit on this call
    return enhancedOkHttpClient.newCall(req).execute();
  }

  private Request buildTokenRequest(URL endpoint) {
    AribaServices aribaServices = new AribaServices(this);
    String mediaType = MediaType.APPLICATION_FORM_URLENCODED;
    RequestBody body = RequestBody.create(TOKEN_GRANT_TYPE,
                                          okhttp3.MediaType.parse(mediaType));
    return new Request.Builder()
      .get().url(endpoint)
      .method(POST, body)
      .addHeader(AUTHORIZATION, String.format("Basic %s", aribaServices.getBase64EncodedValue()))
      .addHeader(CONTENT_TYPE, mediaType)
      .build();
  }

}
