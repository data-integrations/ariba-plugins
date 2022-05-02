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
Feature: Ariba Source - Design time validation scenarios

  @BATCH-TS-ARIBA-DSGN-ERROR-01
  Scenario: Verify required fields missing validation for listed properties
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Click on the Validate button
     # 1 minute wait - To stop rate limit per minute from exceeding.
    And Wait with a timeout of 60 seconds
    Then Verify mandatory property error for below listed properties:
      | referenceName    |
      | baseURL          |
      | realm            |
      | viewTemplateName |
      | clientId         |
      | clientSecret     |
      | apiKey           |

  @BATCH-TS-ARIBA-DSGN-ERROR-02
  Scenario: Verify validation message when user provides invalid Authentication Properties
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "invalid.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "invalid.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "invalid.apikey" for Credentials and Authorization related fields
    And Click on the Validate button
    # 1 minute wait - To stop rate limit per minute from exceeding.
    And Wait with a timeout of 60 seconds
    Then Verify invalid credentials validation message for below listed properties:
      | clientId     |
      | clientSecret |
      | apiKey       |

  @BATCH-TS-ARIBA-DSGN-ERROR-03
  Scenario: Verify validation message for invalid View Template name
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "invalid.viewtemplatenamevalue"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Click on the Validate button
    And Verify that the Plugin is displaying an error message: "invalid.viewtemplatename" on the header
    # 1 minute wait - To stop rate limit per minute from exceeding.
    Then Wait with a timeout of 60 seconds

  @BATCH-TS-ARIBA-DSGN-ERROR-04
  Scenario: Verify validation message for invalid Realm
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "invalid.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Click on the Validate button
    And Verify that the Plugin is displaying an error message: "invalid.realmname" on the header
    # 1 minute wait - To stop rate limit per minute from exceeding.
    Then Wait with a timeout of 60 seconds

  @BATCH-TS-ARIBA-DSGN-ERROR-05
  Scenario: Verify validation message for invalid Date format
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP Ariba" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP Ariba"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.apiendpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "realm" with value: "admin.realm" for Credentials and Authorization related fields
    And Select radio button plugin property: "systemType" with value: "prod"
    And Enter input plugin property: "viewTemplateName" with value: "ProjectFactSystemView"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "apiKey" with value: "admin.apiKey" for Credentials and Authorization related fields
    And Enter input plugin property: "fromDate" with value: "invalid.fromDate"
    And Enter input plugin property: "toDate" with value: "invalid.toDate"
    And Click on the Validate button
    Then Verify that the Plugin Property: "fromDate" is displaying an in-line error message: "invalid.fromDate.message"
    And Verify that the Plugin Property: "toDate" is displaying an in-line error message: "invalid.toDate.message"
    # 1 minute wait - To stop rate limit per minute from exceeding.
    Then Wait with a timeout of 60 seconds