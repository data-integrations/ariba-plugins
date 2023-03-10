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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.ariba.source.AribaBatchSource;
import io.cdap.plugin.ariba.source.AribaServices;
import io.cdap.plugin.ariba.source.AribaStructuredTransformer;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.metadata.AribaResponseContainer;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * AribaConnector Class
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(ResourceConstants.PLUGIN_NAME)
@Description("Connection to access data from Ariba.")
public class AribaConnector implements DirectConnector {
  private static final Logger LOG = LoggerFactory.getLogger(AribaConnector.class);
  private static final String METADATA_PATH = "api/analytics-reporting-view/v1";
  private static final String ENTITY_TYPE_TEMPLATE = "template";
  private static final String VIEW_TEMPLATE = "viewTemplateName";
  private static final String PATH_SEGMENT = "%s/viewTemplates";
  private static final String DATA_PATH_SEGMENT = "%s/views";
  private static final String RECORDS = "Records";
  private static final String TOKEN = "PageToken";
  private static final Gson GSON = new Gson();
  private final AribaConnectorConfig config;
  private static final String AUTHORIZATION = "Authorization";
  private String accessToken;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private ListIterator<JsonNode> jsonNodeListIterator;

  public AribaConnector(AribaConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validateCredentials(collector);
    config.validateToken(collector);
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    JsonArray jsonElements = new JsonArray();
    JsonArray jsonArray = listTemplates(null, jsonElements);
    for (JsonElement jsonElement : jsonArray) {
      JsonObject resultJsonObject = GSON.fromJson(jsonElement, JsonObject.class);
      String name = resultJsonObject.get(VIEW_TEMPLATE).getAsString();
      BrowseEntity.Builder entity = (BrowseEntity.builder(name, name, ENTITY_TYPE_TEMPLATE).
        canBrowse(false).canSample(true));
      browseDetailBuilder.addEntity(entity.build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
    throws IOException {
    AribaServices aribaServices = new AribaServices(config);
    try {
      accessToken = aribaServices.getAccessToken();
    } catch (AribaException e) {
      throw new IOException("Error in generating token", e);
    }
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> properties = new HashMap<>();
    properties.put(io.cdap.plugin.common.ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    String template = connectorSpecRequest.getPath();
    if (template != null) {
      properties.put(AribaPluginConfig.TEMPLATE_NAME, template);
      properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(template));
    }
    try {
      Schema schema = aribaServices.buildOutputSchema(accessToken, template);
      specBuilder.setSchema(schema);
    } catch (AribaException | InterruptedException e) {
      throw new IOException("Error in generating schema", e);
    }
    return specBuilder.addRelatedPlugin(new PluginSpec(AribaBatchSource.NAME, BatchSource.PLUGIN_TYPE,
      properties)).build();
  }

  /**
   * returns the list of all the templates present in Ariba.
   */
  private JsonArray listTemplates(String pageToken, JsonArray jsonElements) throws IOException {
    AribaServices aribaServices = new AribaServices(config);
    try {
      accessToken = aribaServices.getAccessToken();
    } catch (AribaException e) {
      throw new IOException("unable to generate access token", e);
    }
    URL viewTemplatesURL = HttpUrl.parse(config.getBaseURL()).
      newBuilder()
      .addPathSegments(METADATA_PATH)
      .addPathSegments(String.format(PATH_SEGMENT, config.getSystemType()))
      .addQueryParameter(ResourceConstants.PRODUCT, ResourceConstants.ANALYTICS)
      .addQueryParameter(ResourceConstants.PAGE_TOKEN, pageToken)
      .addQueryParameter(ResourceConstants.REALM, config.getRealm()).build().url();
    AribaResponseContainer responseContainer = null;
    try {
      responseContainer = aribaServices.fetchAribaResponse(viewTemplatesURL, accessToken);
    } catch (AribaException | InterruptedException e) {
      throw new IOException("Unable to fetch response", e);
    }
    InputStream responseStream = responseContainer.getResponseBody();
    BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8));
    String result = reader.lines().collect(Collectors.joining(""));
    JsonObject jsonObject = GSON.fromJson(result, JsonObject.class);
    JsonElement nextPageToken = jsonObject.get(TOKEN);
    if (nextPageToken != null) {
      listTemplates(nextPageToken.getAsString(), jsonElements);
      jsonElements.addAll(jsonObject.getAsJsonArray(RECORDS));
    }
    return jsonElements;
  }

  private List<StructuredRecord> listTemplateData(String templateName)
    throws IOException, AribaException, InterruptedException {
    AribaServices aribaServices = new AribaServices(config);
    try {
      accessToken = aribaServices.getAccessToken();
    } catch (AribaException e) {
      throw new IOException("unable to generate access token", e);
    }
    Schema schema = aribaServices.buildOutputSchema(accessToken, templateName);
    URL dataURL = HttpUrl.parse(config.getBaseURL()).
      newBuilder()
      .addPathSegments(METADATA_PATH)
      .addPathSegments(String.format(DATA_PATH_SEGMENT, config.getSystemType()))
      .addPathSegments(templateName)
      .addQueryParameter(ResourceConstants.PRODUCT, ResourceConstants.ANALYTICS)
      .addQueryParameter(ResourceConstants.REALM, config.getRealm()).build().url();
    Request request = new Request.Builder().get().url(dataURL).addHeader(ResourceConstants.API_KEY, config.getApiKey())
      .addHeader(AUTHORIZATION, aribaServices.getAuthenticationKey(accessToken)).build();
    OkHttpClient enhancedOkHttpClient = new OkHttpClient();
    Response response = enhancedOkHttpClient.newCall(request).execute();
    AribaResponseContainer aribaResponseContainer = aribaServices.tokenResponse(response);
    List<JsonNode> nodeData = new ArrayList<>();
    List<StructuredRecord> recordList = new ArrayList<>();
    try (InputStream responseStream = aribaResponseContainer.getResponseBody()) {
      JsonNode nodeRecord = objectMapper.readTree(responseStream);
      for (JsonNode records : nodeRecord) {
        nodeData.add(records);
      }
      jsonNodeListIterator = nodeData.listIterator();
      if (jsonNodeListIterator != null && jsonNodeListIterator.hasNext()) {
        AribaStructuredTransformer transformer = new AribaStructuredTransformer();
        JsonNode row = jsonNodeListIterator.next();
        for (JsonNode record : row) {
          recordList.add(transformer.readFields(record, schema));
        }
      }

      return recordList;
    }

  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
    throws IOException {
    String template = sampleRequest.getPath();
    if (template == null) {
      throw new IllegalArgumentException("Path should contain a template");
    }
    try {
      return listTemplateData(template);
    } catch (AribaException | InterruptedException e) {
      throw new IOException("Unable to fetch the data.", e);
    }
  }
}
