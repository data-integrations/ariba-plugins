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
Feature: Ariba Source - Run time scenarios

  @BATCH-TS-ARIBA-DSGN-RNTM-01 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured for System Type prod
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "SourcingProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Click on the Validate button
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "success"
    Then Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin

  @BATCH-TS-ARIBA-DSGN-RNTM-02 @BQ_SINK_TEST
  Scenario: Verify user should be able to run and deploy the pipeline when plugin is configured for System Type prod
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "SourcingProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Click on the Validate button
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    And Verify count of no of records transferred to the target BigQuery Table
    Then Verify If new record created in Sink application for view template "SourcingProjectFactSystemView" is correct

  @BATCH-TS-ARIBA-DSGN-RNTM-03 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured with Advanced properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "SourcingProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Enter input plugin property: "fromDate" with value: "ariba.fromdate"
    And Enter input plugin property: "toDate" with value: "ariba.todate"
    And Click on the Validate button
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "success"
    Then Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin

  @BATCH-TS-ARIBA-DSGN-RNTM-04 @BQ_SINK_TEST
  Scenario: Verify user should be able to run and deploy the pipeline when plugin is configured with Advanced properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "SourcingProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Enter input plugin property: "fromDate" with value: "ariba.fromdate"
    And Enter input plugin property: "toDate" with value: "ariba.todate"
    And Click on the Validate button
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    And Verify count of no of records transferred to the target BigQuery Table
    Then Verify If new record created in Sink application for view template "SourcingProjectFactSystemView" is correct

