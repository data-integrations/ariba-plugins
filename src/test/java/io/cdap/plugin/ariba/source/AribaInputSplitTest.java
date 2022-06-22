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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 *  Test cases for AribaInputSplit
 */
public class AribaInputSplitTest {

  @Test
  public void testInputSplitWithNonEmptyTableName()  {
    AribaInputSplit aribaInputSplit = new AribaInputSplit("sourceView.zip", "3343ddsfsg3434");
    Assert.assertEquals("sourceView.zip", aribaInputSplit.getFileName());
    Assert.assertEquals("3343ddsfsg3434", aribaInputSplit.getJobId());
  }

  @Test
  public void testInputSplitWithEmptyFileName()  {
    AribaInputSplit aribaInputSplit = new AribaInputSplit();
    Assert.assertEquals(0L, aribaInputSplit.getLength());
    Assert.assertNull(aribaInputSplit.getFileName());
    Assert.assertNull(aribaInputSplit.getJobId());
  }

  @Test
  public void testReadFields() throws IOException {
    AribaInputSplit aribaInputSplit = new AribaInputSplit("sourceView.zip", "3343ddsfsg3434");
    ObjectInputStream objectInputStream = Mockito.mock(ObjectInputStream.class);
    Mockito.when(objectInputStream.readUTF()).thenReturn("Utf");
    aribaInputSplit.readFields(objectInputStream);
    Assert.assertEquals("Utf", aribaInputSplit.getFileName());
    Assert.assertEquals("Utf", aribaInputSplit.getJobId());
  }

  @Test
  public void testWrite() throws IOException {
    AribaInputSplit aribaInputSplit = Mockito.spy(new AribaInputSplit("sourceView.zip",
                                                                                "3343ddsfsg3434"));
    DataOutput dataOutput = Mockito.mock(DataOutput.class);
    aribaInputSplit.write(dataOutput);
    Mockito.verify(aribaInputSplit, Mockito.times(1)).write(dataOutput);
  }
  
  @Test
  public void testGetFileName() {
    AribaInputSplit aribaInputSplit = new AribaInputSplit("FileName", "jobId");
    Assert.assertEquals("FileName", aribaInputSplit.getFileName());
  }

  @Test
  public void testGetJobId() {
    AribaInputSplit aribaInputSplit = new AribaInputSplit("FileName", "5656gfdf");
    aribaInputSplit.getLocations();
    Assert.assertFalse(aribaInputSplit.getFileName().isEmpty());
    Assert.assertEquals("5656gfdf", aribaInputSplit.getJobId());
  }

  @Test(expected = NullPointerException.class)
  public void testWriteWithNullData() throws IOException {
    DataOutput dataOutput = null;
    AribaInputSplit aribaInputSplit = new AribaInputSplit("FileName", "5656gfdf");
    aribaInputSplit.write(dataOutput);
  }

  @Test(expected = NullPointerException.class)
  public void testRead() throws IOException {
    DataInput dataInput = null;
    AribaInputSplit aribaInputSplit = new AribaInputSplit("FileName", "5656gfdf");
    aribaInputSplit.readFields(dataInput);
  }
}
