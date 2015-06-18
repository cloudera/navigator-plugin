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
import static org.mockito.Mockito.*;

import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.UpdatedResults;
import com.cloudera.nav.plugin.client.examples.updatedResults.IncrementalExtractionSample;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * Created by Nadia.Wallace on 6/10/15.
 */

@RunWith(MockitoJUnitRunner.class)
public class UpdatedResultsTest {

  private NavApiCient mockClient;
  private IncrementalExtractionSample ies;

  private String marker1Rep;
  private String marker2Rep;
  private Map<String, Integer> marker2;
  private String allQuery;
  private String markerQuery;
  private List<Map<String, Object>> entities;
  private List<Map<String, Object>> relations;
  private UpdatedResults result;
  //Want to test NavApiCient class, so make mocks of classes that it interacts with-> mock the restTemplate and response

  @Before
  public void setUp() {
    allQuery = "query=identity:*";
    marker1Rep = "{\"identityString\":100}";
    marker2 = Maps.newHashMap();
    marker2.put("abc", 3);
    marker2.put("def", 4);
    MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
    body.put("foo", Lists.newArrayList("*"));

    result = new UpdatedResults(marker1Rep, entities, relations);

    mockClient = mock(NavApiCient.class);
    ies = mock(IncrementalExtractionSample.class);
    Source source1 = new Source("source1", SourceType.HDFS, "cluster1",
        "foo/bar", "identityString", 100);
    when(mockClient.getAllSources()).thenReturn(Lists.newArrayList(source1));
    when(ies.aggUpdatedResults(marker1Rep, allQuery, false)).thenReturn(result); //callRealMethod?
  }


  @Test
  public void testAllUpdates() {
    UpdatedResults ans = new UpdatedResults(marker1Rep, entities, relations); //not mocked, aggUpdatedResults not mocked
    assertEquals(ies.getAllUpdated(false), ans);
  }

  @Test
  public void testIncrementalUpdates() {
    UpdatedResults ans = new UpdatedResults(marker2Rep, entities, relations);
    assertEquals(ans, result);
  }

  @Test
  public void testCurrentMarker() {
    //how to verify correctness?
  }

}
