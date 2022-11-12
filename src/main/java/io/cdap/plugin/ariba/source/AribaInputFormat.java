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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.exception.AribaException;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Input format class to configure splits and record reader
 */
public class AribaInputFormat extends InputFormat<NullWritable, StructuredRecord> {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(AribaInputFormat.class);
  private final List<InputSplit> resultSplits = new ArrayList<>();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    AribaPluginConfig pluginConfig = getPluginConfig(jobContext);
    AribaServices aribaServices = new AribaServices(pluginConfig.getConnection());
    boolean previewEnabled = Boolean.parseBoolean(jobContext.getConfiguration().
                                                    get(ResourceConstants.IS_PREVIEW_ENABLED));

    createJob(pluginConfig, aribaServices, previewEnabled, null);
    return resultSplits;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit inputSplit, TaskAttemptContext
    taskAttemptContext) throws IOException {
    AribaPluginConfig pluginConfig = getPluginConfig(taskAttemptContext);
    AribaServices aribaServices = new AribaServices(pluginConfig.getConnection());
    Schema outputSchema = Schema.parseJson(taskAttemptContext.getConfiguration().get(ResourceConstants.OUTPUT_SCHEMA));
    return new AribaRecordReader(aribaServices, outputSchema, pluginConfig);
  }

  private AribaPluginConfig getPluginConfig(JobContext taskAttemptContext) {
    return GSON.fromJson(taskAttemptContext.getConfiguration().
                           get(ResourceConstants.ARIBA_PLUGIN_PROPERTIES), AribaPluginConfig.class);
  }

  /**
   * call for create Job
   *
   * @param pluginConfig     Plugin config
   * @param aribaServices    Ariba services
   * @param isPreviewEnabled Is preview enabled
   * @param pageToken        Page token
   * @throws IOException
   */

  @VisibleForTesting
  @Nullable
  void createJob(AribaPluginConfig pluginConfig,
                 AribaServices aribaServices, boolean isPreviewEnabled,
                 @Nullable String pageToken) throws IOException {
    JsonNode jobData;
    String jobId;

    try {
      JsonNode createJobResponse = aribaServices.createJob(pluginConfig, pageToken, pluginConfig.getViewTemplateName());
      jobId = createJobResponse.get(ResourceConstants.JOB_ID).asText();
    } catch (AribaException | InterruptedException exception) {
      throw new IOException(exception.getMessage(), exception);
    }

    while (true) {
      try {
        LOG.info("Fetching Data For Job Id: {}", jobId);
        jobData = aribaServices.fetchJobStatus(aribaServices.getAccessToken(), jobId);
        if (jobData.get(ResourceConstants.STATUS).asText().equals(ResourceConstants.COMPLETED) ||
          jobData.get(ResourceConstants.STATUS).asText().equals(ResourceConstants.COMPLETED_ZERO_RECORDS)) {
          break;
        }
        TimeUnit.MINUTES.sleep(2);
      } catch (AribaException | InterruptedException exception) {
        throw new IOException(exception.getMessage(), exception);
      }
    }
    ObjectReader reader = objectMapper.readerFor(new TypeReference<List<String>>() {
    });

    List<String> files = reader.readValue(jobData.get(ResourceConstants.FILES));
    LOG.info("Total Number of files for job id: {} is: {}", jobId, files.size());
    for (String fileName : files) {
      AribaInputSplit inputSplit = new AribaInputSplit(fileName, jobId);
      resultSplits.add(inputSplit);
    }

    LOG.info("Completed page: {} & Total number of pages are: {}",
             jobData.get(ResourceConstants.CURRENT_PAGE), jobData.get(ResourceConstants.TOTAL_PAGES));
    String nextPageToken = jobData.get(ResourceConstants.PAGE_TOKEN).asText();
    LOG.info("Page Token for Next Job is: {}", nextPageToken);
    if (!ResourceConstants.NULL.equalsIgnoreCase(nextPageToken)
      && !isPreviewEnabled) {
      createJob(pluginConfig, aribaServices, isPreviewEnabled, nextPageToken);
    }
  }

}
