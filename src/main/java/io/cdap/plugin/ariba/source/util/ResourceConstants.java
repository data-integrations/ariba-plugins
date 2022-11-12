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

import javax.annotation.Nullable;

/**
 * Contains constant keys for externalized strings, to be used as reference for
 * internationalization/localization of messages and text. The keys when passed
 * to any method of {@link ResourceText}, bring corresponding text message in a
 * language based on the specified or default locale from the ResourceBundle
 * files (i10n).
 */
public enum ResourceConstants {

  // Common resource constants
  ERR_MISSING_PARAM_PREFIX(null, "err.missing.param.prefix"),
  ERR_MISSING_PARAM_OR_MACRO_ACTION(null, "err.missing.param.or.macro.action"),
  ERR_NEGATIVE_PARAM_PREFIX(null, "err.negative.param.prefix"),
  ERR_NEGATIVE_PARAM_ACTION(null, "err.negative.param.action"),
  ERR_CALL_SERVICE_FAILURE(null, "err.call.service.failure"),
  ERR_FETCHING_TOKEN(null, "err.fetching.token"),
  ERR_FETCHING_TOKEN_URL(null, "err.fetching.token.url"),
  ERR_ARIBA_SERVICE_FAILURE("CDF_ARIBA_01501", "err.ariba.service.failure"),
  ERR_MACRO_INPUT(null, "err.call.service.failure"),
  ERR_API_LIMIT_EXCEED_FOR_DAY(null, "err.api.rate.limit.exceeded.for.the.day"),
  ERR_API_LIMIT_EXCEED_FOR_HOUR(null, "err.api.rate.limit.exceeded.for.the.hour"),
  ERR_FIELD_VAL_CONVERT("CDF_SAP_01550", "err.field.val.convert"),
  ERR_NOT_FOUND(null, "err.resource.not.found");

  public static final String FILTER_ERROR = "Either provide both the filter values or provide none";
  public static final String DATE_ERROR = "'From Date' can not be greater then 'To Date'.";
  public static final String FILTER_RANGE = "31536000000";
  public static final String FILTER_RANGE_EXCEED_ERROR = "Filter Range Exceeded, it can not be more then a Year";
  public static final String FILTER_DATE_PARSING_ERROR = "Filter Date Parsing error.";
  public static final String REFERENCE_NAME = "Reference Name";
  public static final String API_END_POINT = "API Endpoint";
  public static final String SYSTEM_TYPE = "System Type";
  public static final String REALM_NAME = "Realm Name";
  public static final String VIEW_TEMPLATE_NAME = "View Template Name";
  public static final String GCP_PROJECT_ID = "GCP Project Id";
  public static final String BUCKET_PATH_LABEL = "Page Token GCS Path";
  public static final String SERVICE_ACCOUNT_JSON = "Service Account GSON";
  public static final String ARRAY = "array";
  public static final String READ = "Read";
  public static final String ARIBA_PLUGIN_PROPERTIES = "aribaPluginProperties";
  public static final String OUTPUT_SCHEMA = "aribaOutputSchema";
  public static final String ENCODED_ENTITY_METADATA_STRING = "aribaEncodedEntityMetaDataString";
  public static final String IS_PREVIEW_ENABLED = "isPreviewEnabled";
  public static final String JOB_ID = "jobId";
  public static final String STATUS = "status";
  public static final String NULL = "null";
  public static final String COMPLETED = "completed";
  public static final String COMPLETED_ZERO_RECORDS = "completedZeroRecords";
  public static final String ERROR_MAX_REACHED = "errorMaxReached";
  public static final String ERROR_INTERNAL = "errorInternal";
  public static final String ERROR_INVALID_DATE_RANGE = "errorInvalidDateRange";
  public static final String PAGE_TOKEN = "pageToken";
  public static final String FILES = "files";
  public static final String STRING = "STRING";
  public static final String NUMBER = "NUMBER";
  public static final String BOOLEAN = "BOOLEAN";
  public static final char HYPHEN = '-';
  public static final String OBJECT = "object";
  public static final String DOT = ".";
  public static final String MESSAGE = "message";
  public static final Integer LIMIT_EXCEED_ERROR_CODE = 4290;
  public static final Integer API_LIMIT_EXCEED = 429;
  public static final String NAME = "name";
  public static final String RETRY_AFTER = "RateLimit-Reset";
  public static final String INVALID_TIME_VALUE = "240000";
  public static final String TOTAL_PAGES = "totalNumOfPages";
  public static final String CURRENT_PAGE = "currentPageNum";
  public static final String PRODUCT = "product";
  public static final String REALM = "realm";
  public static final String ANALYTICS = "analytics";

  public static final Integer DEFAULT_CODE = 1500;
  public static final String PLUGIN_NAME = "Ariba";
  private final String code;
  private final String key;

  ResourceConstants(String code, String key) {
    this.code = code;
    this.key = key;
  }

  @Nullable
  public String getCode() {
    return code;
  }

  public String getKey() {
    return key;
  }

  public String getMsgForKeyWithCode() {
    return getMsgForKey(code);
  }

  public String getMsgForKeyWithCode(Object... params) {
    Object[] destArr = new Object[params.length + 1];
    destArr[0] = code;
    System.arraycopy(params, 0, destArr, 1, params.length);

    return getMsgForKey(destArr);
  }

  public String getMsgForKey() {
    return ResourceText.getString(key);
  }

  public String getMsgForKey(Object... params) {
    return ResourceText.getString(key, params);
  }
}
