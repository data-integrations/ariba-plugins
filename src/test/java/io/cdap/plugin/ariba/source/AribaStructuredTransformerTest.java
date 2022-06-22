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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Test case for AribaStructuredTransformer
 */
public class AribaStructuredTransformerTest {

  @Test
  public void testGetFieldNativeValue() throws IOException {
    AribaStructuredTransformer aribaStructuredTransformer = new AribaStructuredTransformer();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode nodeRecord = mapper.readTree(AribaRecordReaderTest.rowData);
    Object description = aribaStructuredTransformer.getFieldNativeValue(nodeRecord, "Description");
    Assert.assertEquals("test sourcing RFP project", description);
  }

  @Test
  public void testGetSourceSpecificDateValue() {
    AribaStructuredTransformer aribaStructuredTransformer = new AribaStructuredTransformer();
    LocalDate sourceSpecificDateValue = aribaStructuredTransformer.getSourceSpecificDateValue("00010101");
    Assert.assertEquals(LocalDate.parse("0001-01-01"), sourceSpecificDateValue);
  }

  @Test
  public void testHandleConversionException() {
    AribaStructuredTransformer aribaStructuredTransformer = new AribaStructuredTransformer();
    try {
      aribaStructuredTransformer.handleConversionException("schema", "name", "test", new Exception());
      Assert.fail("testHandleConversionException expected to fail, but succeeded");
    } catch (IOException io) {
      Assert.assertEquals("CDF_SAP_01550 - Error while converting field 'name' having value " +
                            "'test' to schema", io.getMessage());
    }
  }

  @Test
  public void testHandleMinusAtEnd() {
    AribaStructuredTransformer aribaStructuredTransformer = new AribaStructuredTransformer();
    String data = aribaStructuredTransformer.handleMinusAtEnd("12345-");
    Assert.assertEquals("-12345", data);
  }

  @Test
  public void testSourceSpecificTimeValue() {
    AribaStructuredTransformer aribaStructuredTransformer = new AribaStructuredTransformer();
    LocalTime sourceSpecificTimeValue = aribaStructuredTransformer.getSourceSpecificTimeValue("230000");
    Assert.assertEquals("23:00", sourceSpecificTimeValue.toString());
  }
}
