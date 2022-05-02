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

  @BATCH-TS-ARIBA-DSGN-RNTM-MACRO-01 @BQ_SINK @FILE_PATH @BQ_SINK_CLEANUP @BQ_TEMP_CLEANUP
  Scenario: Verify data is transferred correctly to BQ for System Type prod with macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "baseURL" and set the value to: "apiendpoint"
    And Click on the Macro button of Property: "realm" and set the value to: "realm"
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "viewTemplateName" and set the value to: "viewTemplateName"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "apiKey" and set the value to: "apiKey"
    And Validate "SAP Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqtarget.table"
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-Ariba" and sink as "BigQueryTable" to establish connection
    # 1 minute wait - Before deploying the pipeline.
    And Wait with a timeout of 60 seconds
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value from environment variable "admin.apiendpoint" for key "apiendpoint"
    And Enter runtime argument value from environment variable "admin.realm" for key "realm"
    And Enter runtime argument value "ariba.viewtemplatename" for key "viewTemplateName"
    And Enter runtime argument value from environment variable "admin.clientid" for key "clientId"
    And Enter runtime argument value from environment variable "admin.clientsecret" for key "clientSecret"
    And Enter runtime argument value from environment variable "admin.apiKey" for key "apiKey"
    And Run the Pipeline in Runtime with runtime arguments
    # Custom wait - To let the pipeline run and then check for its status.
    And Wait with a timeout of 960 seconds
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    And Close the pipeline logs
    Then Validate record created in Sink application for ViewTemplate is equal to expected output file "expectedOutputFile"
    # 1 minute wait - To stop rate limit per minute from exceeding.
    And Wait with a timeout of 60 seconds

  @BATCH-TS-ARIBA-RNTM-MACRO-02 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid ViewTemplateName with Macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "viewTemplateName" and set the value to: "viewTemplateName"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Validate "SAP Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqtarget.table"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.viewtemplatenamevalue" for key "viewTemplateName"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    # Custom wait - To let the pipeline run and then check for its status.
    And Wait with a timeout of 240 seconds
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.viewtemplate.logsmessage          |

  @BATCH-TS-ARIBA-RNTM-MACRO-03 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid Realm with Macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ariba.viewtemplatename"
    And Click on the Macro button of Property: "realm" and set the value to: "realm"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Validate "SAP Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqtarget.table"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.realm" for key "realm"
    And Run the Pipeline in Runtime with runtime arguments
    # Custom wait - To let the pipeline run and then check for its status.
    And Wait with a timeout of 240 seconds
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.realm.logsmessage                 |

  @BATCH-TS-ARIBA-RNTM-MACRO-04 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid Credential Details with Macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "apiKey" and set the value to: "apiKey"
    And Enter input plugin property: "viewTemplateName" with value: "ariba.viewtemplatename"
    And Validate "SAP Ariba" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqtarget.table"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-Ariba" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.clientid" for key "clientId"
    And Enter runtime argument value "invalid.clientsecret" for key "clientSecret"
    And Enter runtime argument value "invalid.apikey" for key "apiKey"
    And Run the Pipeline in Runtime with runtime arguments
    # Custom wait - To let the pipeline run and then check for its status.
    And Wait with a timeout of 240 seconds
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.credentials.logsmessage           |