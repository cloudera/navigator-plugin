package com.cloudera.nav.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

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
  private ParameterizedTypeReference<Map<String, Object>[]> marker1;
  private Map<String, Integer> marker2;

 //Want to test NavApiCient class, so make mocks of classes that it interacts with-> mock the restTemplate and response
  @Before
  public void setUp(){
    mockClient = mock(NavApiCient.class);
    marker1 =  new ParameterizedTypeReference<Map<String, Object>[]>(){};

    //when(mockClient.navResponse("entities", "?query=identity:*", marker1)).thenReturn(null);

  }

  @Test
  public void testAllUpdates(){

  }

  @Test
  public void testIncrementalUpdates(){

  }

}
