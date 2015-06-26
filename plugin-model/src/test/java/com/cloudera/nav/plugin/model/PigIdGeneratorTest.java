/*
 * Copyright (c) 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.nav.plugin.model;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Maps;

import java.util.Map;

import org.apache.commons.configuration.MapConfiguration;
import org.junit.Before;

public class PigIdGeneratorTest {

  @Before
  public void testOperationId() {
    String jobName = "Brain";
    String plan = "The same thing we do every night pinky";
    Map<String, Object> map = Maps.newHashMap();
    map.put(PigIdGenerator.PIG_LOGICAL_PLAN_HASH_PROP, plan);
    MapConfiguration jobConf = new MapConfiguration(map);
    String id = PigIdGenerator.generateNewOperationId(jobName, jobConf);
    assertEquals(MD5IdGenerator.generateIdentity(jobName, plan), id);
  }

  @Before
  public void testExecId() {
    Map<String, Object> map = Maps.newHashMap();
    String script = "acme.plan.no.2012374587";
    map.put(PigIdGenerator.PIG_SCRIPT_ID_PROP, script);
    MapConfiguration jobConf = new MapConfiguration(map);
    String id = PigIdGenerator.generateExecutionId(jobConf);
    assertEquals(MD5IdGenerator.generateIdentity(script), id);
  }
}
