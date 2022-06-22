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

package io.cdap.plugin.ariba.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This {@code AribaStructuredTransformer} contains the logic to
 * convert Ariba record to {@code StructuredRecord}
 */
public class AribaStructuredTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(AribaStructuredTransformer.class);

  public StructuredRecord readFields(JsonNode nativeRawRecord, Schema outputSchema) {
    StructuredRecord.Builder recBuilder = StructuredRecord.builder(outputSchema);
    List<Schema.Field> cdapFields = outputSchema.getFields();

    cdapFields.forEach(field -> {
      String fieldName = field.getName();
      Object fieldVal = getFieldNativeValue(nativeRawRecord, fieldName);
      Schema childSchema = field.getSchema();

//       eg.
//        {
//          "Owner": {
//            "SourceSystem": "ASM",
//            "UserId": "customersupportadmin",
//            "PasswordAdapter": "PasswordAdapter"
//            }
//        }
      if (nativeRawRecord.get(fieldName) != null &&
        nativeRawRecord.get(fieldName).getNodeType().name()
          .equalsIgnoreCase(ResourceConstants.OBJECT) && !childSchema.getType().isSimpleType()) {
        fieldVal = readFields((JsonNode) fieldVal, childSchema.getUnionSchema(0));
      }

//       e.g.
//       {
//         "Suppliers":[{
//            "Suppliers": {
//              "SourceSystem": "ASM",
//              "SupplierId": "",
//              "SupplierLocationId": ""
//              }
//           }]
//       }
      if (nativeRawRecord.get(fieldName) != null &&
        nativeRawRecord.get(fieldName).getNodeType().name()
          .equalsIgnoreCase(ResourceConstants.ARRAY) && !childSchema.getType().isSimpleType()) {
        fieldVal = readInternalDeltaFeed(childSchema, (JsonNode) fieldVal);
      }

//         "State": "Active"
      if (fieldVal != null) {
        processSchemaTypeValue(childSchema, recBuilder, fieldName, fieldVal);
      }
    });
    return recBuilder.build();
  }

  private List<StructuredRecord> readInternalDeltaFeed(Schema recordSchema, JsonNode fieldValStr) {
    ObjectMapper mapper = new ObjectMapper();
    if (fieldValStr.isArray() && recordSchema.getNonNullable().getComponentSchema() != null) {
      try {
        List<JsonNode> jsonList = mapper.readValue(fieldValStr.toString(), new TypeReference<List<JsonNode>>() {
        });
        return jsonList.stream().map(
          field -> readFields(field.get(recordSchema.getNonNullable().getComponentSchema().getRecordName()),
                              recordSchema.getNonNullable().getComponentSchema())).collect(Collectors.toList());
      } catch (JsonProcessingException e) {
        LOG.error("Error in Processing Structured Transformer from Json, cause: {}", e.getMessage());
      }
    }
    return Collections.emptyList();
  }


  private void processSchemaTypeValue(Schema fieldSchema, StructuredRecord.Builder recordBuilder,
                                      String fieldName, Object fieldValue) {

    // Get Non-nullable schema object for the current field
    Schema nonNullSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    if (fieldValue instanceof StructuredRecord || fieldValue instanceof List) {
      recordBuilder.set(fieldName, fieldValue);
    } else if (fieldValue instanceof String) {
      boolean isBlank = handleBlankVal(recordBuilder, fieldName, (String) fieldValue);

      // If value is NOT null or empty then transform acc. to the field's data type
      if (!isBlank) {
        try {
          processValue(nonNullSchema, recordBuilder, fieldName, (String) fieldValue);
        } catch (IOException e) {
          LOG.error("Error in Processing Schema type value, cause: {}", e.getMessage());
        }
      }
    } else {
      recordBuilder.set(fieldName, null);
    }
  }

  @Nullable
  @VisibleForTesting
  Object getFieldNativeValue(JsonNode nativeRawRecord, String fieldName) {
    JsonNode recordValue = nativeRawRecord.get(fieldName);
    if (recordValue != null) {
      if (nativeRawRecord.get(fieldName).getNodeType().name().equalsIgnoreCase(ResourceConstants.ARRAY) ||
        nativeRawRecord.get(fieldName).getNodeType().name().equalsIgnoreCase(ResourceConstants.OBJECT)) {
        return nativeRawRecord.get(fieldName);

      } else if (nativeRawRecord.get(fieldName).getNodeType().name().equals(ResourceConstants.STRING) ||
        nativeRawRecord.get(fieldName).getNodeType().name().equals(ResourceConstants.NUMBER) ||
        nativeRawRecord.get(fieldName).getNodeType().name().equals(ResourceConstants.BOOLEAN)) {

        return nativeRawRecord.get(fieldName).asText();

      }
    }
    return null;
  }

  /**
   * Checks and sets null or original (whitespace filled) string as value in the
   * {@code StructuredRecord.Builder} for field represented by {@code fieldName}
   *
   * @param recBuilder
   * @param fieldName
   * @param fieldValStr
   * @return True, if native value is null or empty. False, otherwise.
   */
  @VisibleForTesting
  boolean handleBlankVal(StructuredRecord.Builder recBuilder, String fieldName,
                         @Nullable String fieldValStr) {

    if (fieldValStr == null || fieldValStr.trim().isEmpty()) {
      // If no non-whitespace char is present in value and field type is String, then
      // set original value (consisting of only whitespace) to CDAP field
      recBuilder.set(fieldName, fieldValStr);
      return true;
    }

    return false;
  }

  /**
   * Processes field native value according to their type based on Schema Simple
   * Type or Logical Type
   *
   * @param nonNullSchema
   * @param recBuilder
   * @param encodedFieldName
   * @param fieldValStr
   * @throws IOException
   */
  private void processValue(Schema nonNullSchema, StructuredRecord.Builder recBuilder, String encodedFieldName,
                            String fieldValStr)
    throws IOException {

    try {
      if (nonNullSchema.getLogicalType() != null) {
        processLogicalTypeVal(nonNullSchema, recBuilder, encodedFieldName, fieldValStr.trim());
      } else {
        processTypeVal(nonNullSchema.getType(), recBuilder, encodedFieldName, fieldValStr);
      }
    } catch (Exception e) {
      handleConversionException(nonNullSchema.getLogicalType() != null ? nonNullSchema.getLogicalType().toString()
                                  : nonNullSchema.getType().toString(), encodedFieldName, fieldValStr, e);
    }
  }

  /**
   * Process the value for field which is mapped to a {@code Schema.LogicalType}
   * and set into the {@code StructuredRecord.Builder}.
   *
   * @param nonNullSchema non nullable Schema
   * @param recBuilder    Structured record builder
   * @param fieldName     Ariba objects's field name (may be encoded to remove CDAP
   *                      unsupported chars)
   * @param fieldValTrim  trimmed value corresponding to an Ariba object's field
   *                      name
   */
  private void processLogicalTypeVal(Schema nonNullSchema, StructuredRecord.Builder recBuilder, String fieldName,
                                     String fieldValTrim) {

    switch (Objects.requireNonNull(nonNullSchema.getLogicalType())) {
      case DECIMAL:
        fieldValTrim = handleMinusAtEnd(fieldValTrim);
        recBuilder.setDecimal(fieldName, new BigDecimal(fieldValTrim).setScale(nonNullSchema.getScale()));
        break;

      case DATE:
        recBuilder.setDate(fieldName, getSourceSpecificDateValue(fieldValTrim));
        break;

      case TIME_MICROS:
        recBuilder.setTime(fieldName, getSourceSpecificTimeValue(fieldValTrim));
        break;

      case TIMESTAMP_MICROS:
        ZonedDateTime zonedDateTime = null;
        // Check if UTCLONG string having format yyyy-MM-dd HH:mm:ss'Z' does not
        // start with default date value part 0000
        if (!fieldValTrim.startsWith("0000")) {
          String parsableTimestamp = fieldValTrim.replace(' ', 'T');
          zonedDateTime = ZonedDateTime.parse(parsableTimestamp, DateTimeFormatter.ISO_DATE_TIME);
          zonedDateTime = zonedDateTime.plus(0, ChronoUnit.NANOS);
        }
        recBuilder.setTimestamp(fieldName, zonedDateTime);
        break;

      default:
        recBuilder.set(fieldName, fieldValTrim);
        break;
    }
  }

  /**
   * Process the value for field which is mapped to a {@code Schema.Type} and set
   * into the {@code StructuredRecord.Builder}.
   *
   * @param fieldType   Schema field logical type
   * @param recBuilder  Structured record builder
   * @param fieldName   Ariba object's field name (may be encoded to remove CDAP
   *                    unsupported chars)
   * @param fieldValStr value corresponding to an Ariba object's field
   */
  private void processTypeVal(Schema.Type fieldType, StructuredRecord.Builder recBuilder, String fieldName,
                              String fieldValStr) {

    String colValTrim = fieldValStr.trim();
    switch (fieldType) {
      case INT:
        colValTrim = handleMinusAtEnd(colValTrim);
        recBuilder.set(fieldName, Integer.parseInt(colValTrim));
        break;

      case LONG:
        colValTrim = handleMinusAtEnd(colValTrim);
        recBuilder.set(fieldName, Long.parseLong(colValTrim));
        break;

      case DOUBLE:
        colValTrim = handleMinusAtEnd(colValTrim);
        recBuilder.set(fieldName, Double.parseDouble(colValTrim));
        break;

      case BYTES:
        recBuilder.set(fieldName, Bytes.toBytesBinary(colValTrim));
        break;

      case BOOLEAN:
        recBuilder.set(fieldName, Boolean.parseBoolean(colValTrim));
        break;

      case STRING:
        recBuilder.set(fieldName, fieldValStr);
        break;

      case NULL:
        recBuilder.set(fieldName, null);
        break;

      default:
        // shouldn't ever get here
        String err =
          ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(fieldName,
                                                                       fieldValStr, "any Schema type");

        throw new UnexpectedFormatException(err);
    }
  }

  @VisibleForTesting
  void handleConversionException(String schemaTypeString, String fieldName, String fieldVal, Exception e)
    throws IOException {

    String err = ResourceConstants.ERR_FIELD_VAL_CONVERT.getMsgForKeyWithCode(fieldName, fieldVal, schemaTypeString);
    throw new IOException(err, e);
  }

  /**
   * Removes minus sign from the end of the string and puts it at the beginning.
   * This handling is required for number values because some profile settings in
   * Ariba may result in the minus sign being put at the end of the number like
   * {@code 12345-}.
   *
   * @param fieldValTrim Trimmed field native value
   * @return String with minus appended at the beginning if it was at the end.
   * Else returns the same string
   */
  @VisibleForTesting
  String handleMinusAtEnd(String fieldValTrim) {
    char lastChar = fieldValTrim.charAt(fieldValTrim.length() - 1);
    if (lastChar == ResourceConstants.HYPHEN) {
      StringBuilder sb = new StringBuilder(fieldValTrim);
      sb.deleteCharAt(sb.length() - 1).insert(0, lastChar);
      fieldValTrim = sb.toString();
    }

    return fieldValTrim;
  }


  @Nullable
  @VisibleForTesting
  LocalDate getSourceSpecificDateValue(String rawDateValue) {
    LocalDate localDate = null;
    // Date field in Ariba may have default/uninitialized value starting with 0000
    if (!rawDateValue.startsWith("0000")) {
      localDate = LocalDate.parse(rawDateValue, DateTimeFormatter.BASIC_ISO_DATE);
    }
    return localDate;
  }

  @VisibleForTesting
  LocalTime getSourceSpecificTimeValue(String rawTimeValue) {
    // Handle invalid time values = 240000. It is invalid and must be rolled over to
    // 000000. Any other invalid values > 235959, must simply throw an error.
    if (ResourceConstants.INVALID_TIME_VALUE.equals(rawTimeValue)) {
      rawTimeValue = "000000";
    }
    StringBuilder timeValBuilder = new StringBuilder(rawTimeValue);

    // Time field in Ariba has values in format HHmmss, so add colon separators to
    // make it parseable
    timeValBuilder.insert(4, ':').insert(2, ':');
    return LocalTime.parse(timeValBuilder, DateTimeFormatter.ISO_LOCAL_TIME);
  }
}
