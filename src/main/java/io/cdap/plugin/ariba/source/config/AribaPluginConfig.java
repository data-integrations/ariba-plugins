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

package io.cdap.plugin.ariba.source.config;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.ariba.source.util.AribaUtil;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.annotation.Nullable;

/**
 * This {@code AribaPluginConfig} contains all Ariba configuration parameters.
 */

public class AribaPluginConfig extends ReferencePluginConfig {

  public static final String BASE_URL = "baseURL";
  public static final String SYSTEM_TYPE = "systemType";
  public static final String REALM = "realm";
  public static final String TEMPLATE_NAME = "viewTemplateName";
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String APIKEY = "apiKey";
  public static final String FROM_DATE = "fromDate";
  public static final String TO_DATE = "toDate";
  public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
  public static final String REFERENCE_NAME = "referenceName";

  private static final Logger LOG = LoggerFactory.getLogger(AribaPluginConfig.class);
  private static final String COMMON_ACTION = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();
  /**
   * Basic parameters.
   */
  @Macro
  @Description("Base Path of Ariba API.")
  private final String baseURL;

  @Description("Type of system the Ariba instance is running on: Production or Sandbox.")
  private final String systemType;

  @Macro
  @Description("Realm name from which data is to be extracted.")
  private final String realm;

  @Macro
  @Description("Name of the view template from which data is to be extracted.")
  private final String viewTemplateName;

  /**
   * Credentials parameters
   */

  @Macro
  @Description("Ariba Client ID.")
  private final String clientId;

  @Macro
  @Description("Ariba Client Secret.")
  private final String clientSecret;

  @Macro
  @Description("Ariba Application Key.")
  private final String apiKey;

  /**
   * Advanced parameters
   */

  @Nullable
  @Macro
  @Description("Start date of the extract")
  private final String fromDate;

  @Nullable
  @Macro
  @Description("End date of the extract")
  private final String toDate;


  public AribaPluginConfig(String referenceName,
                           String baseURL,
                           String systemType,
                           String realm,
                           String viewTemplateName,
                           String clientId,
                           String clientSecret,
                           String apiKey,
                           @Nullable String fromDate,
                           @Nullable String toDate) {

    super(referenceName);
    this.baseURL = baseURL;
    this.systemType = systemType;
    this.realm = realm;
    this.viewTemplateName = viewTemplateName;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.apiKey = apiKey;
    this.fromDate = fromDate;
    this.toDate = toDate;
  }

  public String getSystemType() {
    return systemType;
  }

  public String getRealm() {
    return realm;
  }

  public String getViewTemplateName() {
    return viewTemplateName;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getApiKey() {
    return apiKey;
  }

  public String getReferenceName() {
    return this.referenceName;
  }

  public String getBaseURL() {
    return trim(this.baseURL);
  }

  @Nullable
  public String getFromDate() {
    return fromDate;
  }

  @Nullable
  public String getToDate() {
    return toDate;
  }

  /**
   * Validates the given {@code AribaPluginConfig} and throws the relative error messages.
   *
   * @param failureCollector {@code FailureCollector}
   */
  public void validatePluginParameters(FailureCollector failureCollector) {

    LOG.debug("Validating mandatory parameters.");
    validateMandatoryParameters(failureCollector);

    // Validates the given referenceName to consists of characters allowed to represent a dataset.
    IdUtils.validateReferenceName(referenceName, failureCollector);

    LOG.debug("Validating Security Type parameters.");
    validateCredentials(failureCollector);

    LOG.debug("Validating the advanced parameters.");
    if (AribaUtil.isNotNullOrEmpty(fromDate) || AribaUtil.isNotNullOrEmpty(toDate)) {
      validateAdvanceParameters(failureCollector);
    }

    failureCollector.getOrThrowException();
  }


  /**
   * Validates the mandatory parameters.
   *
   * @param failureCollector {@code FailureCollector}
   */
  private void validateMandatoryParameters(FailureCollector failureCollector) {

    if (AribaUtil.isNullOrEmpty(getReferenceName())) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.REFERENCE_NAME);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(REFERENCE_NAME);
    }
    if (AribaUtil.isNullOrEmpty(getBaseURL()) && !containsMacro(BASE_URL)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.API_END_POINT);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(BASE_URL);
    }
    if (AribaUtil.isNullOrEmpty(getSystemType()) && !containsMacro(SYSTEM_TYPE)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.SYSTEM_TYPE);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(SYSTEM_TYPE);
    }
    if (AribaUtil.isNullOrEmpty(getRealm()) && !containsMacro(REALM)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.REALM_NAME);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(REALM);
    }
    if (AribaUtil.isNullOrEmpty(getViewTemplateName()) && !containsMacro(TEMPLATE_NAME)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(ResourceConstants.VIEW_TEMPLATE_NAME);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(TEMPLATE_NAME);
    }
  }

  /**
   * Validates the credentials parameters.
   *
   * @param failureCollector {@code FailureCollector}
   */
  private void validateCredentials(FailureCollector failureCollector) {

    if (AribaUtil.isNullOrEmpty(getClientId()) && !containsMacro(CLIENT_ID)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba Client Id");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(CLIENT_ID);
    }
    if (AribaUtil.isNullOrEmpty(getClientSecret()) && !containsMacro(CLIENT_SECRET)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba Client Secret");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(CLIENT_SECRET);
    }
    if (AribaUtil.isNullOrEmpty(getApiKey()) && !containsMacro(APIKEY)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Ariba API Key");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(APIKEY);
    }
  }

  /**
   * Validates the advance parameters.
   *
   * @param failureCollector {@code FailureCollector}
   */
  private void validateAdvanceParameters(FailureCollector failureCollector) {

    DateFormat aribaDateFormat = new SimpleDateFormat(DATE_FORMAT);
    String action = ResourceConstants.ERR_NEGATIVE_PARAM_ACTION.getMsgForKey();

    if (AribaUtil.isNotNullOrEmpty(fromDate) && AribaUtil.isValidDateFormat(fromDate)
      && !containsMacro(FROM_DATE)) {
      String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey("From date");
      failureCollector.addFailure(errMsg, action).withConfigProperty(FROM_DATE);
    }

    if (AribaUtil.isNotNullOrEmpty(toDate) && AribaUtil.isValidDateFormat(toDate)
      && !containsMacro(TO_DATE)) {
      String errMsg = ResourceConstants.ERR_NEGATIVE_PARAM_PREFIX.getMsgForKey("To date");
      failureCollector.addFailure(errMsg, action).withConfigProperty(TO_DATE);
    }

    if ((AribaUtil.isNotNullOrEmpty(fromDate) && AribaUtil.isNullOrEmpty(toDate)) ||
      (AribaUtil.isNotNullOrEmpty(toDate) && AribaUtil.isNullOrEmpty(fromDate))) {
      failureCollector.addFailure(ResourceConstants.FILTER_ERROR,
                                  action).withConfigProperty(TO_DATE).withConfigProperty(FROM_DATE);
    }

    try {
      if (failureCollector.getValidationFailures().isEmpty()) {
        Date startDate = aribaDateFormat.parse(fromDate);
        Date endDate = aribaDateFormat.parse(toDate);
        if (startDate.after(endDate)) {
          failureCollector.addFailure(ResourceConstants.DATE_ERROR,
                                      action).withConfigProperty(TO_DATE).withConfigProperty(FROM_DATE);
        }
        if ((endDate.getTime() - startDate.getTime() > Long.parseLong(ResourceConstants.FILTER_RANGE))) {
          failureCollector.addFailure(ResourceConstants.FILTER_RANGE_EXCEED_ERROR,
                                      action).withConfigProperty(TO_DATE).withConfigProperty(FROM_DATE);
        }
      }
    } catch (ParseException e) {
      failureCollector.addFailure(ResourceConstants.FILTER_DATE_PARSING_ERROR,
                                  action).withConfigProperty(TO_DATE).withConfigProperty(FROM_DATE);
    }
  }

  /**
   * Checks if the call to Ariba service is required for metadata creation.
   * condition parameters: ['host' | 'Realm' | 'Template' | 'Client Id' | 'Client Secret']
   * - any parameter is 'macro' then it returns 'false'
   *
   * @return boolean flag as per the check
   */
  public boolean isSchemaBuildRequired() {
    LOG.debug("Checking output schema creation is required or not.");
    return !containsMacro(BASE_URL) && !containsMacro(REALM) &&
      !containsMacro(SYSTEM_TYPE) && !containsMacro(TEMPLATE_NAME) && !containsMacro(CLIENT_ID)
      && !containsMacro(CLIENT_SECRET) && !containsMacro(APIKEY);
  }

  /**
   * Trim whitespace from the beginning and end of a string.
   *
   * @param rawString
   * @return trimmed String or null
   */
  private String trim(String rawString) {
    if (AribaUtil.isNotNullOrEmpty(rawString)) {
      return rawString.trim();
    }
    return rawString;
  }
}
