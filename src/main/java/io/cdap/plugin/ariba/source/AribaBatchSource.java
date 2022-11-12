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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.connector.AribaConnector;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.util.AribaUtil;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads data from Ariba
 */

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(AribaBatchSource.NAME)
@Description("Reads data from SAP Ariba.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = ResourceConstants.PLUGIN_NAME)})
public class AribaBatchSource extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {

  public static final String NAME = "Ariba";
  private static final Logger LOG = LoggerFactory.getLogger(AribaBatchSource.class);
  private static final Gson GSON = new Gson();
  private final AribaPluginConfig pluginConfig;
  private final AribaServices aribaServices;
  public String accessToken;

  public AribaBatchSource(AribaPluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
    aribaServices = new AribaServices(pluginConfig.getConnection());
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
            getOutputSchema());
        }
      } catch (IOException | InterruptedException exception) {
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
    FailureCollector collector = context.getFailureCollector();
    pluginConfig.validatePluginParameters(collector); // validate when macros are already substituted
    collector.getOrThrowException();

    Schema outputSchema = context.getOutputSchema();
    if (outputSchema == null) {
      outputSchema = getOutputSchema();
    }
    if (outputSchema == null) {
      throw new IllegalArgumentException(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode());
    }
    setJobForDataRead(context, outputSchema);
    emitLineage(context, outputSchema, pluginConfig.getViewTemplateName());
    collector.getOrThrowException();
  }

  @Nullable
  private Schema getOutputSchema() throws IOException, AribaException, InterruptedException {
    String token = aribaServices.getAccessToken();
    LOG.trace("Initiating Metadata Call To Ariba");
    return aribaServices.buildOutputSchema(token, pluginConfig.getViewTemplateName());
  }

  private void setJobForDataRead(BatchSourceContext context, Schema outputSchema) throws IOException {
    Job job = JobUtils.createInstance();
    Configuration jobConfiguration = job.getConfiguration();
    // Set plugin properties in Hadoop Job's configuration
    jobConfiguration.set(ResourceConstants.ARIBA_PLUGIN_PROPERTIES, GSON.toJson(pluginConfig));
    // Setting plugin output schema
    jobConfiguration.set(ResourceConstants.OUTPUT_SCHEMA, outputSchema.toString());

    jobConfiguration.set(ResourceConstants.ENCODED_ENTITY_METADATA_STRING, outputSchema.toString());
    jobConfiguration.set(ResourceConstants.IS_PREVIEW_ENABLED, String.valueOf(context.isPreviewEnabled()));

    SourceInputFormatProvider inputFormat = new SourceInputFormatProvider(AribaInputFormat.class, jobConfiguration);
    context.setInput(Input.of(pluginConfig.getReferenceName(), inputFormat));
  }

  private void emitLineage(BatchSourceContext context, Schema schema, String entity) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, pluginConfig.getReferenceName());
    lineageRecorder.createExternalDataset(schema);

    if (schema.getFields() != null) {
      String operationDesc = String.format("Read '%s' from ariba service '%s'", entity,
                                           pluginConfig.getViewTemplateName());

      lineageRecorder.recordRead(ResourceConstants.READ, operationDesc,
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }


  /**
   * Checks and attaches the UI fields with its relevant error message.
   *
   * @param aribaException   {@code AribaException}
   * @param failureCollector {@code FailureCollector}
   */
  @VisibleForTesting
  protected void attachFieldWithError(AribaException aribaException, FailureCollector failureCollector) {

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
