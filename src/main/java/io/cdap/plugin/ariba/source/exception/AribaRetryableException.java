/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.ariba.source.exception;

/**
 * This {@code AribaRetryableException} class is used to capture all the errors that are related to Ariba
 * API limit issues.
 */

public class AribaRetryableException extends Exception {

  private final Integer errorCode;

  public AribaRetryableException(String message) {
    this(message, null, null);
  }

  public AribaRetryableException(String message, Integer errorCode) {
    this(message, errorCode, null);
  }

  public AribaRetryableException(String message, Throwable cause) {
    this(message, null, cause);
  }

  public AribaRetryableException(String message, Integer errorCode, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public Integer getErrorCode() {
    return this.errorCode;
  }
}
