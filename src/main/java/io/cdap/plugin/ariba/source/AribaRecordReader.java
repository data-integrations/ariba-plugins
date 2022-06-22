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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.ariba.source.exception.AribaException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * A {@link AribaRecordReader} contains Hadoop Job RecordReader implementation
 */
public class AribaRecordReader extends RecordReader<NullWritable, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(AribaRecordReader.class);
  private final AribaServices aribaServices;
  private final Schema outputSchema;
  private final AribaStructuredTransformer transformer;
  private int pos;
  private JsonNode row;
  private ListIterator<JsonNode> jsonNodeListIterator;

  public AribaRecordReader(AribaServices aribaServices, Schema outputSchema) {
    this.aribaServices = aribaServices;
    this.outputSchema = outputSchema;
    this.transformer = new AribaStructuredTransformer();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext taContext) throws IOException {
    AribaInputSplit aribaInputSplit = (AribaInputSplit) split;
    List<JsonNode> nodeData = new ArrayList<>();
    try {
      JsonNode nodeRecord = aribaServices.fetchData(aribaInputSplit.getJobId(), aribaInputSplit.getFileName());
      for (JsonNode records : nodeRecord) {
        nodeData.add(records);
      }
      jsonNodeListIterator = nodeData.listIterator();
      LOG.info("Data size is: {} in file: {} and jobId: {}",
               nodeData.size(), aribaInputSplit.getFileName(), aribaInputSplit.getJobId());
      if (nodeData.isEmpty()) {
        LOG.info("Fetch Data Response of jobId: {} , and File Name: {} , with no records is: {} ",
                 aribaInputSplit.getJobId(), aribaInputSplit.getFileName(), nodeRecord);
      }
    } catch (AribaException | InterruptedException exception) {
      throw new IOException(exception.getMessage(), exception);
    }
  }

  @Override
  public boolean nextKeyValue() {
    if (jsonNodeListIterator != null && jsonNodeListIterator.hasNext()) {
      row = jsonNodeListIterator.next();
      pos++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public StructuredRecord getCurrentValue() {
    return transformer.readFields(row, outputSchema);
  }

  @Override
  public float getProgress() {
    return pos;
  }

  @Override
  public void close() {
    // No-op
  }

}
