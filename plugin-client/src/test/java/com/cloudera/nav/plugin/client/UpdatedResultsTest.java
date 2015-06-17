package com.cloudera.nav.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.cloudera.nav.plugin.client.writer.MetadataWriterFactory;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.core.ParameterizedTypeReference;

/**
 * Created by Nadia.Wallace on 6/10/15.
 */

@RunWith(MockitoJUnitRunner.class)
public class UpdatedResultsTest {

  private NavApiCient mockClient;
  private Map<String, Integer> marker1;
  private String markerRep;
  private String allQuery;
  private String markerQuery;
  private List<Map<String, Object>> entities;
  private List<Map<String, Object>> relations;
  private UpdatedResults result;
 //Want to test NavApiCient class, so make mocks of classes that it interacts with-> mock the restTemplate and response

  @Before
  public void setUp(){
    allQuery = "query=identity:*";
    result = new UpdatedResults(markerRep,  entities, relations);
    mockClient = mock(NavApiCient.class);
    //when(mockClient.getCurrentMarker()).thenReturn();
    //when(mockClient.getAllPages("entities", markerQuery).thenReturn();
    //when(mockClient.getAllPages("relations", markerQuery)).thenReturn();

  }

  @Test
  public void testExtractorQuery(){
    //String ans = mockClient.getExtractorQueryString(marker1, marker2); //not mocked
    //assertEquals(ans, markerQuery);
  }


  @Test
  public void testAllUpdates(){
    //UpdatedResults ans = new UpdatedResults('foo', ); //not mocked, aggUpdatedResults not mocked
    //assertEquals(mockClient.getAllUpdated(), ans);
  }

  @Test
  public void testIncrementalUpdates(){

  }

  @Test
  public void testCurrentMarker(){
    //how to verify correctness?
  }

}
