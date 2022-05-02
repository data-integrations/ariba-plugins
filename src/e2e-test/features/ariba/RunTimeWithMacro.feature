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
Feature: Ariba Source - Run time scenarios (macros)

  @BATCH-TS-ARIBA-DSGN-RNTM-MACRO-01 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured for System Type prod with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "baseURL" and set the value to: "apiendpoint"
    And Click on the Macro button of Property: "realm" and set the value to: "realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "viewTemplateName" and set the value to: "viewTemplateName"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "apiKey" and set the value to: "apiKey"
    And Click on the Validate button
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "admin.apiendpoint" for key "apiendpoint"
    And Enter runtime argument value "admin.realm" for key "realm"
    And Enter runtime argument value "ariba.viewtemplatename" for key "viewTemplateName"
    And Enter runtime argument value "admin.clientid" for key "clientId"
    And Enter runtime argument value "admin.clientsecret" for key "clientSecret"
    And Enter runtime argument value "admin.apiKey" for key "apiKey"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-ARIBA-RNTM-MACRO-02 @BQ_SINK_TEST
  Scenario: Verify user should be able to run and deploy the pipeline when plugin is configured for System Type prod with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "baseURL" and set the value to: "apiendpoint"
    And Click on the Macro button of Property: "realm" and set the value to: "realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "viewTemplateName" and set the value to: "viewTemplateName"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "apiKey" and set the value to: "apiKey"
    And Validate "Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "admin.apiendpoint" for key "apiendpoint"
    And Enter runtime argument value "admin.realm" for key "realm"
    And Enter runtime argument value "ariba.viewtemplatename" for key "viewTemplateName"
    And Enter runtime argument value "admin.clientid" for key "clientId"
    And Enter runtime argument value "admin.clientsecret" for key "clientSecret"
    And Enter runtime argument value "admin.apiKey" for key "apiKey"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Verify count of no of records transferred to the target BigQuery Table

  @BATCH-TS-ARIBA-DSGN-RNTM-MACRO-03 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured with Advanced properties with macros
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
    And Validate "Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "ariba.fromdate" for key "fromDate"
    And Enter runtime argument value "ariba.todate" for key "toDate"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-ARIBA-RNTM-MACRO-04 @BQ_SINK_TEST
  Scenario: Verify user should be able to run and deploy the pipeline when plugin is configured with Advanced properties with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint"
    And Enter input plugin property: "realm" with value: "admin.realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "RequisitionLineItemFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "apiKey" with value: "admin.apiKey"
    And Click on the Macro button of Property: "fromDate" and set the value to: "fromDate"
    And Click on the Macro button of Property: "toDate" and set the value to: "toDate"
    And Validate "Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "ariba.fromdate" for key "fromDate"
    And Enter runtime argument value "ariba.todate" for key "toDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Verify count of no of records transferred to the target BigQuery Table

  @BATCH-TS-ARIBA-RNTM-MACRO-05 @BQ_SINK_TEST
  Scenario: Verify pipeline failure message in logs when user provides invalid ViewTemplateName with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint"
    And Enter input plugin property: "realm" with value: "admin.realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "viewTemplateName" and set the value to: "viewTemplateName"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "apiKey" with value: "admin.apiKey"
    And Validate "Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.viewtemplatenamevalue" for key "viewTemplateName"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entry having Level as: "ERROR" and Message as: "invalid.viewtemplate.logsmessage"

  @BATCH-TS-ARIBA-RNTM-MACRO-06 @BQ_SINK_TEST
  Scenario: Verify pipeline failure message in logs when user provides invalid Realm with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint"
    And Enter input plugin property: "viewTemplateName" with value: "ariba.viewtemplatename"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "realm" and set the value to: "realm"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "apiKey" with value: "admin.apiKey"
    And Validate "Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.realm" for key "realm"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entry having Level as: "ERROR" and Message as: "invalid.realm.logsmessage"

  @BATCH-TS-ARIBA-RNTM-MACRO-07 @BQ_SINK_TEST
  Scenario: Verify pipeline failure message in logs when user provides invalid Credential Details with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint"
    And Enter input plugin property: "realm" with value: "admin.realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ariba.viewtemplatename"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "apiKey" and set the value to: "apiKey"
    And Validate "Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.clientid" for key "clientId"
    And Enter runtime argument value "invalid.clientsecret" for key "clientSecret"
    And Enter runtime argument value "invalid.apikey" for key "apiKey"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entry having Level as: "ERROR" and Message as: "invalid.credentials.logsmessage"

  @BATCH-TS-ARIBA-RNTM-MACRO-08 @BQ_SINK_TEST
  Scenario: Verify pipeline failure message in logs when user provides invalid Advanced Properties with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint"
    And Enter input plugin property: "realm" with value: "admin.realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ariba.viewtemplatename"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "apiKey" with value: "admin.apiKey"
    And Click on the Macro button of Property: "fromDate" and set the value to: "fromDate"
    And Click on the Macro button of Property: "toDate" and set the value to: "toDate"
    And Validate "Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.fromDate" for key "fromDate"
    And Enter runtime argument value "invalid.toDate" for key "toDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entry having Level as: "ERROR" and Message as: "invalid.dateformat.logsmessage"




