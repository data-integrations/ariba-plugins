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
package io.cdap.plugin.ariba.source.metadata;

import io.cdap.plugin.ariba.source.exception.AribaException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class ResourceContainerTest {
  AribaResponseContainer aribaResponseContainer;
  String httpStatusMsg = "Not found";
  byte[] targetStream;
  AribaResponseContainer.Builder columnDetail;

  @Before
  public void setUp() {
    targetStream = httpStatusMsg.getBytes();
    columnDetail = AribaResponseContainer.builder();
  }

  @Test
  public void testGetHttpStatusCode() {
    aribaResponseContainer = new AribaResponseContainer(404, httpStatusMsg, targetStream);
    Assert.assertEquals(404, aribaResponseContainer.getHttpStatusCode());
  }

  @Test
  public void testGetHttpStatusMsg() {
    aribaResponseContainer = new AribaResponseContainer(404, httpStatusMsg, targetStream);
    Assert.assertEquals(httpStatusMsg, aribaResponseContainer.getHttpStatusMsg());
  }

  @Test
  public void testGetByteStream() throws IOException {
    aribaResponseContainer = new AribaResponseContainer(404, httpStatusMsg, targetStream);
    InputStream inputStream = new ByteArrayInputStream(targetStream);
    Assert.assertEquals(inputStream.read(), Objects.requireNonNull(aribaResponseContainer.getResponseBody()).read());
    Assert.assertEquals(targetStream, columnDetail.responseStream(targetStream).responseStream);
  }

  @Test
  public void testHttpStatusMsg() {
    Assert.assertEquals("Not found", columnDetail.httpStatusMsg("Not found").httpStatusMsg);
  }

  @Test
  public void testHttpStatusCode() {
    Assert.assertEquals(404, columnDetail.httpStatusCode(404).httpStatusCode);
  }

  @Test
  public void testAribaException() {
    AribaException aribaException = new AribaException("unable to access");
    Assert.assertEquals("unable to access", aribaException.getMessage());
    AribaException aribaExceptionUnauthorized = new AribaException("unAuthorized", new Throwable());
    Assert.assertEquals("unAuthorized", aribaExceptionUnauthorized.getMessage());
  }

}
