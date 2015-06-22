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

package com.cloudera.nav.plugin.client.examples.updatedResults;

import com.cloudera.com.fasterxml.jackson.core.type.TypeReference;
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import com.cloudera.nav.plugin.client.ClientUtils;
import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.client.ResultsBatch;
import com.cloudera.nav.plugin.client.UpdatedResults;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Created by Nadia.Wallace on 6/16/15.
 */
public class IncrementalExtractionSample {

  private NavApiCient client;
  private PluginConfigurations config;
  private Map<String, Integer> currentMarker;

  public IncrementalExtractionSample(NavApiCient client){
    this.client = client;
    this.config = client.getConfig();
  }

  /** Returns all of the entities and relations in the database,
   * plus a marker to denote when this search took place
   *
   * @return
   */
  public UpdatedResults getAllUpdated(){
    UpdatedResults updatedResults;
    currentMarker = getCurrentMarker();
    try {
      String currentMarkerRep = new ObjectMapper().writeValueAsString(currentMarker);
      String queryString = "identity:*";
      updatedResults = aggUpdatedResults(currentMarkerRep, queryString);
      return updatedResults;
    } catch (IOException e){
      throw Throwables.propagate(e);
    }
  }



  /**Returns all of the entities and relations in the database that have been
   * updated or added since the source iterations indicated by the marker
   *
   * @param markerRep JSON representation of sourceId : sourceExtractIteration
   * @return
   */

  public UpdatedResults getAllUpdated(String markerRep){
    UpdatedResults updatedResults;
    TypeReference<Map<String, Integer>> typeRef =
        new TypeReference<Map<String, Integer>>(){};
    currentMarker = getCurrentMarker(); //makes getAllSources() call
    try {
      String currentMarkerRep =
          new ObjectMapper().writeValueAsString(currentMarker);
      Map<String, Integer> marker =
          new ObjectMapper().readValue(markerRep, typeRef);
      String extractorQueryString = getExtractorQueryString(marker, currentMarker);
      updatedResults = aggUpdatedResults(currentMarkerRep, extractorQueryString);
      return updatedResults;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }


  /** Constructs a query in Solr syntax as "<extractorRunId1> OR <extractorRunId2> ..."
   * for all extraction iterations per source between m1 and m2
   *
   * @param m1
   * @param m2
   * @return
   */
  private String getExtractorQueryString(Map<String, Integer> m1,
                                         Map<String, Integer> m2){
    String queryString = "extractorRunId:(";
    for (String key: m1.keySet()){
      for (int i=m1.get(key); i<(m2.get(key)+1); i++){
        String possible = key + "##" + Integer.toString(i);
        queryString = queryString + possible + " OR ";
      }
    }
    return queryString.substring(0, queryString.length()-4)+")";
  }

  /** Constructs an UpdatedResults object with results of getAllPages
   * for entities and relations, and the marker used to generate these results.
   *
   * @param markerRep
   * @param queryString
   * @return
   *
   * Public for testing only
   */
  public UpdatedResults aggUpdatedResults(String markerRep,String queryString){
    UpdatedResults updatedResults;
    Iterable<Map<String, Object>> entities =
        new IncrementalExtractIterable<>(this, "entities", queryString, 100);
    Iterable<Map<String, Object>> relations =
        new IncrementalExtractIterable<>(this, "relations", queryString, 100);
    updatedResults = new UpdatedResults(markerRep, entities, relations);
    return updatedResults;
  }

  /** Constructs url from a type (entity or relation), query, and cursorMark.
   *  Returns a response batch.
   * Called in next() of IncrementalExtractIterator()
   *
   * @param type
   * @param queryString
   * @return
   */
  public <T> ResultsBatch<T> getResultsBatch(String type,
                                      String queryString,
                                      String cursorMark){

    String fullUrlPost = ClientUtils.getUrl(config, type);
    Map<String, String> body = Maps.newHashMap();
    body.put("query", queryString);
    body.put("cursorMark", cursorMark);
    ResultsBatch<T> response = navResponse(fullUrlPost, body);
    return response;
  }

  /**
   * USED FOR POST METHOD
   *
   * public for testing only
   */
  @VisibleForTesting
  public <T> ResultsBatch<T> navResponse(String url,  Map<String,String> formData){
    ParameterizedTypeReference<ResultsBatch<T>> resultClass =
        new ParameterizedTypeReference<ResultsBatch<T>>(){};
    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = ClientUtils.getAuthHeaders(config);
    HttpEntity<Map<String, String>> request =new HttpEntity<>(formData, headers);
    ResponseEntity<ResultsBatch<T>> response = restTemplate.exchange(url,
        HttpMethod.POST, request, resultClass);
    ResultsBatch<T> responseResult = response.getBody();
    return responseResult;
  }

  /** Generate marker from each source and its sourceExtractIteration
   *
   * @return
   *
   * Public for testing
   */
  private Map<String, Integer> getCurrentMarker(){
    Collection<Source> sources = client.getAllSources();
    HashMap<String, Integer> newMarker = Maps. newHashMap();
    for (Source source : sources){
      if(source.getSourceType().equals(SourceType.IMPALA)){
        continue;
      }
      String id = source.getIdentity();
      Integer sourceExtractIteration = source.getSourceExtractIteration();
      newMarker.put(id, sourceExtractIteration);
    }
    return newMarker;
  }

  public String getMarker(){
    Map<String, Integer> currentMarker = getCurrentMarker();
    try {
      String rep = new ObjectMapper().writeValueAsString(currentMarker);
      return rep;
    } catch (IOException e){
      throw Throwables.propagate(e);
    }
  }
}
