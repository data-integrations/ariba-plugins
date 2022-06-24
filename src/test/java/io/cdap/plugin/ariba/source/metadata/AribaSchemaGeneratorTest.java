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

package io.cdap.plugin.ariba.source.metadata;

import io.cdap.plugin.ariba.source.util.ResourceConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AribaSchemaGeneratorTest {

  @Test
  public void testAribaSchemaGenerator() {
    AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();
    columnDetail.viewTemplateName("name2")
      .name("name").isPrimaryKey(false).type(ResourceConstants.OBJECT).size(0)
      .isCustomField(false).scale(0).precision(0)
      .childList(null);
    AribaColumnMetadata columnList = columnDetail.build();
    AribaSchemaGenerator aribaSchemaGenerator = new AribaSchemaGenerator(Collections.singletonList(columnList));
    Assert.assertEquals("name", Objects.requireNonNull(
      aribaSchemaGenerator.buildSchema().getFields()).get(0).getName());
  }

  @Test
  public void buildSchemaFieldTest() {
    AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();
    List<AribaColumnMetadata> childList = new ArrayList<>();
    columnDetail.viewTemplateName("name2")
      .name("testName").isPrimaryKey(false).type(ResourceConstants.OBJECT).size(0)
      .isCustomField(false).scale(0).precision(0)
      .childList(null);
    AribaColumnMetadata columnList = columnDetail.build();

    childList.add(columnList);
    AribaColumnMetadata.Builder columnDetail1 = AribaColumnMetadata.builder();
    columnDetail1.viewTemplateName("name2")
      .name("testName").isPrimaryKey(false).type(ResourceConstants.OBJECT).size(0)
      .isCustomField(false).scale(0).precision(0)
      .childList(childList);
    AribaColumnMetadata columnList1 = columnDetail1.build();

    AribaSchemaGenerator aribaSchemaGenerator = new AribaSchemaGenerator(Collections.singletonList(columnList1));
    aribaSchemaGenerator.buildSchemaField(columnList1);
    AribaColumnMetadata.Builder columnDetail2 = AribaColumnMetadata.builder();
    columnDetail2.viewTemplateName("name2")
      .name("testSampleName").isPrimaryKey(false).type(ResourceConstants.ARRAY).size(0)
      .isCustomField(false).scale(0).precision(0).childList(childList);
    AribaColumnMetadata columnList2 = columnDetail2.build();

    AribaSchemaGenerator aribaSchemaGenerator2 = new AribaSchemaGenerator(Collections.singletonList(columnList2));
    aribaSchemaGenerator2.buildSchemaField(columnList2);

    Assert.assertEquals("testName", Objects.requireNonNull(
      aribaSchemaGenerator.buildSchema().getFields()).get(0).getName());
    Assert.assertEquals("testSampleName", Objects.requireNonNull(
      aribaSchemaGenerator2.buildSchema().getFields()).get(0).getName());
  }

  @Test
  public void testAribaColumnMetadata() {
    AribaColumnMetadata.Builder columnDetail = AribaColumnMetadata.builder();

    columnDetail.viewTemplateName("name2")
      .name("name").isPrimaryKey(false).type(ResourceConstants.OBJECT).size(5)
      .isCustomField(false).scale(2).precision(3)
      .childList(null);
    AribaColumnMetadata columnList = columnDetail.build();

    Assert.assertEquals("name", columnList.getName());
    Assert.assertEquals("name2", columnList.getViewTemplateName());
    Assert.assertNull(columnList.getChildList());
    Assert.assertFalse(columnList.isPrimaryKey());
    Assert.assertEquals("object", columnList.getType());
    Assert.assertEquals(3, columnList.getPrecision());
    Assert.assertEquals(2, columnList.getScale());
    Assert.assertFalse(columnList.isCustomField());
    Assert.assertEquals(5, columnList.getSize());
  }
}
