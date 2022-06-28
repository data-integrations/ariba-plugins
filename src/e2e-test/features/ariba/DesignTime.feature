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
Feature: Ariba Source - Design time scenarios

  @BATCH-TS-ARIBA-DSGN-01
  Scenario Outline: Verify user should be able to validate output schema for the plugin when System Type is prod
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "<ViewTemplateName>"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Click on the Validate button
    Then Verify the Output Schema matches the Expected Schema: "<ExpectedSchema>"
    Examples:
      | ViewTemplateName                  | ExpectedSchema                                      |
      | SourcingProjectFactSystemView     | aribasourceschema.SourcingProjectFactSystemView     |
      | ProjectFactSystemView             | aribasourceschema.ProjectFactSystemView             |
      | SourcingRequestFactSystemView     | aribasourceschema.SourcingRequestFactSystemView     |
      | SPMProjectFactSystemView          | aribasourceschema.SPMProjectFactSystemView          |

  @BATCH-TS-ARIBA-DSGN-02
  Scenario: Verify user should be able to validate the plugin for Advanced properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Enter input plugin property: "fromDate" with value: "ariba.fromdate"
    And Enter input plugin property: "toDate" with value: "ariba.todate"
    Then Validate "Ariba" plugin properties

  @BATCH-TS-ARIBA-DSGN-03
  Scenario: Verify user should be able to validate the plugin for Expected date format properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Verify Expected date format Text for: "fromDate"
    And Verify Expected date format Text for: "toDate"
    Then Validate "Ariba" plugin properties

  @BATCH-TS-ARIBA-DSGN-04
  Scenario: Verify user should be able to validate the plugin for Record type field
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
    And Validate "Ariba" plugin properties
    Then Verify the Output Schema matches the Expected Schema for listed Hierarchical fields:
      | FieldName            | SchemaJsonArray              |
      | Owner                | schema.record.owner |
      | DependsOnProject     | schema.record.dop   |

  @BATCH-TS-ARIBA-DSGN-05
  Scenario: Verify user should be able to validate the plugin for Array type field
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
    And Validate "Ariba" plugin properties
    Then Verify the Output Schema matches the Expected Schema for listed Hierarchical fields:
      | FieldName            | SchemaJsonArray               |
      | Region               | schema.array.region           |

