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

package io.cdap.plugin.aribasource.actions;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.aribasource.locators.AribaSourcePropertiesPage;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents - Ariba - Source plugin - Properties page - Actions.
 */
public class AribaSourcePropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(AribaSourcePropertiesPageActions.class);

  public static void verifyExpectedDateFormatFromPlaceholder(String datacyValue) {
    String actualText = ElementHelper.getElementAttribute(
      AribaSourcePropertiesPage.getWebElement(datacyValue), "placeholder");
    String expectedText = PluginPropertyUtils.errorProp("message." + datacyValue);

    logger.info("Verifying that the element: " + AribaSourcePropertiesPage.getWebElement(datacyValue) +
                  " contains text: " + expectedText);
    logger.info("Actual text: " + actualText);
    Assert.assertEquals(expectedText, actualText);
  }
}
