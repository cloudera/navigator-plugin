package com.cloudera.nav.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.cloudera.nav.plugin.client.writer.MetadataWriterFactory;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.net.URI;
import java.net.URL;
import java.util.Collection;
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
  private UpdatedResults result;
 //Want to test NavApiCient class, so make mocks of classes that it interacts with-> mock the restTemplate and response

  @Before
  public void setUp(){
    allQuery = "query=identity:*";
    mockClient = mock(NavApiCient.class);
    //when(mockClient.getCurrentMarker()).thenReturn();
    //when(mockClient.getAllPages(markerRep, markerQuery).thenReturn();

  }

  @Test
  public void testExtractorQuery(){
    //String ans = mockClient.getExtractorQueryString(marker1, marker2); //not mocked
    //assertEquals(ans, markerQuery);
  }


  @Test
  public void testAllUpdates(){
    //UpdatedResults ans = mockClient.getAllUpdated(); //not mocked, aggUpdatedResults not mocked
    //assertEquals(mockClient.getAllUpdated())
  }

  @Test
  public void testIncrementalUpdates(){

  }

  @Test
  public void testCurrentMarker(){
    //how to verify correctness?
  }

}
