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

package io.cdap.plugin.ariba.source.util;

import io.cdap.plugin.ariba.source.exception.AribaException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.annotation.Nullable;

/**
 * Utility class for common/repetitive logic and operations.
 */
public class AribaUtil {

  private AribaUtil() {
  }

  /**
   * Checks a {@code CharSequence} instance for {@code NOT null && NOT empty}.
   *
   * @param charSeq which needs to be checked
   * @return the boolean result of
   * {@code (charSeq != null && !charSeq.toString().isEmpty())}
   */
  public static boolean isNotNullOrEmpty(@Nullable CharSequence charSeq) {
    return charSeq != null && !charSeq.toString().isEmpty();
  }

  /**
   * Checks any {@code CharSequence} instance for {@code null || empty}.
   *
   * @param charSeq which needs to be checked
   * @return the boolean result of
   * {@code (charSeq == null || charSeq.toString().isEmpty())}
   */
  public static boolean isNullOrEmpty(@Nullable CharSequence charSeq) {
    return !isNotNullOrEmpty(charSeq);
  }

  /**
   * Check the given timestamp correctness. E.g. 2021-12-01T03:04:23Z
   *
   * @param timeStamp from the plugin config.
   * @return boolean result of the check.
   */
  public static boolean isValidDateFormat(String dateFormat, String timeStamp) {
    final SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    try {
      final Date date = sdf.parse(timeStamp);

      if (!timeStamp.equals(sdf.format(date))) {
        return false;
      }
    } catch (ParseException e) {
      return false;
    }
    return true;

  }

  /**
   * Builds a user friendly error message for any {@code AribaException} exception
   *
   * @param aribaException {@code AribaException}
   * @return user friendly error message
   */
  public static String buildAribaServiceError(AribaException aribaException) {
    StringBuilder errorDetails = new StringBuilder()
      .append(aribaException.getMessage())
      .append(" ");

    if (aribaException.getCause() != null) {
      errorDetails.append(aribaException.getCause().getMessage());
    }
    return errorDetails.toString();
  }
}
