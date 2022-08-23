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

package io.cdap.plugin.aribasource.stepsdesign;

import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.aribasource.actions.AribaSourcePropertiesPageActions;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;

import java.io.IOException;

/**
 * Represents - Ariba - Source plugin - Properties page - Steps.
 */
public class DesignTimeSteps {

  @And("Verify Expected date format Text for: {string}")
  public void verifyExpectedDateFormat(String datacyValue) {
    AribaSourcePropertiesPageActions.verifyExpectedDateFormatFromPlaceholder(datacyValue);
  }

  @Then("Verify If new record created in Sink application for view template {string} is correct")
  public void verifyIfNewRecordCreatedInSinkApplicationForViewTemplateIsCorrect(String viewTemplateName)
    throws IOException, InterruptedException {
    String aribaRecordJsonArray = PluginPropertyUtils.pluginProp(viewTemplateName + ".body");
    AribaSourcePropertiesPageActions.verifyIfRecordCreatedInSinkIsCorrect(aribaRecordJsonArray);
  }
}
