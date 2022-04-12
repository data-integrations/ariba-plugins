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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.util.AribaUtil;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * A {@link BatchSource} that reads data from Ariba
 */

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(AribaBatchSource.NAME)
@Description("Reads data from Sap Ariba.")
public class AribaBatchSource extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {

  public static final String NAME = "Ariba";
  private static final Logger LOG = LoggerFactory.getLogger(AribaBatchSource.class);
  private final AribaPluginConfig pluginConfig;
  private final AribaServices aribaServices;
  public String accessToken;

  public AribaBatchSource(AribaPluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
    aribaServices = new AribaServices(pluginConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    LOG.trace("Initiating Ariba batch source configure pipeline");
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    pluginConfig.validatePluginParameters(failureCollector);

    if (pluginConfig.isSchemaBuildRequired()) {
      try {
        accessToken = aribaServices.getAccessToken();
        // Get metadata if connection is successful
        if (accessToken != null) {
          pipelineConfigurer.getStageConfigurer().setOutputSchema(
            aribaServices.buildOutputSchema(accessToken));
        }
      } catch (IOException exception) {
        failureCollector.addFailure(exception.getMessage(), null);
        failureCollector.getOrThrowException();
      } catch (AribaException exception) {
        attachFieldWithError(exception, failureCollector);
        failureCollector.getOrThrowException();
      }
    } else {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // TODO: https://cdap.atlassian.net/browse/PLUGIN-1172
  }


  /**
   * Checks and attaches the UI fields with its relevant error message.
   *
   * @param aribaException   {@code AribaException}
   * @param failureCollector {@code FailureCollector}
   */
  private void attachFieldWithError(AribaException aribaException, FailureCollector failureCollector) {

    switch (aribaException.getErrorCode()) {
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        String errMsg = AribaUtil.buildAribaServiceError(aribaException);
        failureCollector.addFailure(errMsg, null).withConfigProperty(AribaPluginConfig.APIKEY);
        failureCollector.addFailure(errMsg, null).withConfigProperty(AribaPluginConfig.CLIENT_ID);
        failureCollector.addFailure(errMsg, null).withConfigProperty(AribaPluginConfig.CLIENT_SECRET);
        break;

      case HttpURLConnection.HTTP_NOT_FOUND:
      case HttpURLConnection.HTTP_BAD_REQUEST:
        errMsg = ResourceConstants.ERR_ARIBA_SERVICE_FAILURE.getMsgForKeyWithCode(
          AribaUtil.buildAribaServiceError(aribaException));
        failureCollector.addFailure(errMsg, ResourceConstants.ERR_NOT_FOUND.getMsgForKey());
        break;

      default:
        errMsg = ResourceConstants.ERR_ARIBA_SERVICE_FAILURE.getMsgForKeyWithCode(
          AribaUtil.buildAribaServiceError(aribaException));
        failureCollector.addFailure(errMsg, null);
    }
  }
}
