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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Ariba InputSlipt class for creating splits
 */
public class AribaInputSplit extends InputSplit implements Writable {

  private String fileName;
  private String jobId;

  public AribaInputSplit() { }

  public AribaInputSplit(String fileName, String jobId) {
    this.fileName = fileName;
    this.jobId = jobId;
  }

  public String getFileName() {
    return fileName;
  }

  public String getJobId() {
    return jobId;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(this.fileName);
    dataOutput.writeUTF(this.jobId);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.fileName = dataInput.readUTF();
    this.jobId = dataInput.readUTF();
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }
}
