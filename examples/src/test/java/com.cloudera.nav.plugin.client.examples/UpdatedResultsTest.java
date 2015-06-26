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

package com.cloudera.nav.plugin.client.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.client.examples.updatedResults.IncrementalExtractIterable;
import com.cloudera.nav.plugin.client.examples.updatedResults.ResultsBatch;
import com.cloudera.nav.plugin.client.examples.updatedResults.UpdatedResults;
import com.cloudera.nav.plugin.client.examples.updatedResults.IncrementalExtractionSample;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.*;

/**
 * Created by Nadia.Wallace on 6/10/15.
 */

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class UpdatedResultsTest {

  private NavApiCient client;
  private IncrementalExtractionSample ies;

  private String marker1Rep;
  private String marker2Rep;
  private Map<String, Integer> marker2;
  private String allQuery;
  private String markerQuery;
  private IncrementalExtractIterable<Map<String, Object>> entities;
  private IncrementalExtractIterable<Map<String, Object>> relations;
  private UpdatedResults result;
  private URI uri;
  private ResultsBatch resultBatch;


  @Before
  public void setUp() {
    URL url = this.getClass().getClassLoader().getResource("nav_plugin.conf");
    PluginConfigurations config =new PluginConfigurations();
    config.setUsername("admin");
    config.setPassword("admin");
    config.setNavigatorUrl("navigator/url");
    client = mock(NavApiCient.class);
    when(client.getConfig()).thenReturn(config);
    ies = new IncrementalExtractionSample(client);

    allQuery = "query=identity:*";
    marker1Rep = "{\"identityString\":100}";
    marker2 = Maps.newHashMap();
    marker2.put("abc", 3);
    marker2.put("def", 4);
    result = new UpdatedResults(marker1Rep, entities, relations);
    resultBatch = new ResultsBatch();
    resultBatch.setCursorMark("*");
    Map<String, Object> map1 = Maps.newHashMap();
    List<Map<String, Object>> mapArr = Lists.newArrayList(map1);
    resultBatch.setResults(mapArr);

    Source source1 = new Source("source1", SourceType.HDFS, "cluster1",
        "foo/bar", "identityString", 100);

    when(client.getAllSources()).thenReturn(Lists.newArrayList(source1));
    Map<String, String> testBody = Maps.newHashMap();
    testBody.put("query", "identity:*");
    testBody.put("cursorMark", "nextCursor");
  }

  @Test
  public void testAllUpdates() {
    UpdatedResults res = ies.getAllUpdated();
    assertTrue(res!=null);
  }

  @Test
  public void testIncrementalUpdates() {
    UpdatedResults res = ies.getAllUpdated(marker1Rep);
    assertTrue(res!=null);
    assertEquals(res.getMarker(), marker1Rep);
  }

  @Test
  public void testIncrementalQuery(){
    String entityQuery = "sourceType:HDFS";
    String relationQuery = "type: PARENT_CHILD";
    UpdatedResults res = ies.getAllUpdated(marker1Rep, entityQuery, relationQuery);
    assertTrue(res!=null);
    assertEquals(res.getMarker(), marker1Rep);
  }

  @Test
  public void testCurrentMarker() {
    String res = ies.getMarker();
    assertEquals(marker1Rep, res);
  }

}
