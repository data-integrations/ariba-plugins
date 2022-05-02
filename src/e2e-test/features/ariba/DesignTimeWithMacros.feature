# Copyright © 2022 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


@AribaSource
@Smoke
@Regression
Feature: Ariba Source - Design time scenarios (macros)

  @BATCH-TS-ARIBA-DSGN-MACRO-01
  Scenario:Verify user should be able to validate the plugin when Authentication properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint"
    And Enter input plugin property: "realm" with value: "admin.realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ProjectFactSystemView"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "apiKey" and set the value to: "apiKey"
    Then Validate "Ariba" plugin properties

  @BATCH-TS-ARIBA-DSGN-MACRO-02
  Scenario:Verify user should be able to validate the plugin when Basic properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "baseURL" and set the value to: "baseURL"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "realm" and set the value to: "realm"
    And Click on the Macro button of Property: "viewTemplateName" and set the value to: "viewTemplateName"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "apiKey" with value: "admin.apiKey"
    Then Validate "Ariba" plugin properties


  @BATCH-TS-ARIBA-DSGN-MACRO-03
  Scenario:Verify user should be able to validate the plugin when Advanced properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint"
    And Enter input plugin property: "realm" with value: "admin.realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "apiKey" with value: "admin.apiKey"
    And Click on the Macro button of Property: "fromDate" and set the value to: "fromDate"
    And Click on the Macro button of Property: "toDate" and set the value to: "toDate"
    Then Validate "Ariba" plugin properties
