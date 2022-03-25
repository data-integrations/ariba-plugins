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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * This {@code AribaResponseContainer} container class is used to contains request body of type {@code InputStream},
 * along with the following:
 * - HTTP STATUS CODE,
 * - HTTP STATUS MESSAGE
 */

public class AribaResponseContainer {

  private final int httpStatusCode;
  private final String httpStatusMsg;
  private final byte[] responseBody;

  public AribaResponseContainer(int httpStatusCode, String httpStatusMsg, @Nullable byte[] responseBody) {

    this.httpStatusCode = httpStatusCode;
    this.httpStatusMsg = httpStatusMsg;
    this.responseBody = responseBody;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getHttpStatusCode() {
    return this.httpStatusCode;
  }

  public String getHttpStatusMsg() {
    return this.httpStatusMsg;
  }

  @Nullable
  public InputStream getResponseBody() {
    return new ByteArrayInputStream(responseBody);
  }

  /**
   * Helper class to simplify {@link AribaResponseContainer} class creation.
   */
  public static class Builder {
    private int httpStatusCode;
    private String httpStatusMsg;
    private byte[] responseStream;

    public Builder httpStatusCode(int httpStatusCode) {
      this.httpStatusCode = httpStatusCode;
      return this;
    }

    public Builder httpStatusMsg(String httpStatusMsg) {
      this.httpStatusMsg = httpStatusMsg;
      return this;
    }

    public Builder responseStream(byte[] responseStream) {
      this.responseStream = responseStream;
      return this;
    }

    public AribaResponseContainer build() {
      return new AribaResponseContainer(this.httpStatusCode, this.httpStatusMsg, this.responseStream);
    }
  }
}
