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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.ariba.source.config.AribaPluginConfig;
import io.cdap.plugin.ariba.source.util.ResourceConstants;
import mockit.Mocked;
import mockit.Tested;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class AribaBatchSourceTest {

  @Tested
  private static AribaBatchSource aribaBatchSource;
  private MockPipelineConfigurer pipelineConfigurer;
  private AribaPluginConfig pluginConfig;
  private AribaServices aribaServices;
  private Schema schema;

  @Mocked
  private BatchSourceContext context;

  @Before
  public void setUp() {
    pipelineConfigurer = new MockPipelineConfigurer(null);
    pluginConfig = new AribaPluginConfig("unit-test-ref-name", "http://localhost",
                                         "testDocument", "testTemplate",
                                         "realm", "analytics",
                                         "clientId", "clientSecret",
                                         "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
  }

  @Test
  public void testConfigurePipelineWithInvalidBasicParam() {
    pluginConfig = new AribaPluginConfig("", "",
                                         "prod", "",
                                         "", "clientId",
                                         "clientSecret", "apiKey",
                                         "2022-01-28T10:05:02Z", "2022-01-31T10:05:02Z");
    try {
      aribaBatchSource = new AribaBatchSource(pluginConfig);
      aribaBatchSource.configurePipeline(pipelineConfigurer);
      Assert.fail("Expected exception is not thrown");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals("Failures size does not match", 5, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Reference Name"),
                          failures.get(0).getMessage());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("API Endpoint"),
                          failures.get(1).getMessage());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Realm Name"),
                          failures.get(2).getMessage());

      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("View Template Name"),
                          failures.get(3).getMessage());
    }
  }

}
