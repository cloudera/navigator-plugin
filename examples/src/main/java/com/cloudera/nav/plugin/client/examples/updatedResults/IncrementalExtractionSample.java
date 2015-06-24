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
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
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

/** Sample for showing incremental extraction using the plugin client library. The
 * getAllUpdated() method has signatures for calling with and without a marker,
 * and with specifying a filter query string for entities and relations to be
 * returned.
 *
 * If no marker is passed in, all entities and relations will be returned. Obtain
 * a starting marker from the result of getAllUpdated() with {result}.getMarker().
 *
 */
public class IncrementalExtractionSample {

  private NavApiCient client;
  private PluginConfigurations config;
  private Map<String, Integer> currentMarker;
  private final String DEFAULT_QUERY;
  private List<String> emptyRunIds = Lists.newArrayList();

  public IncrementalExtractionSample(NavApiCient client){
    this.client = client;
    this.config = client.getConfig();
    this.DEFAULT_QUERY = "identity:*";
  }

  /** Returns all of the entities and relations in the database,
   * plus a marker to denote when this search took place
   *
   * @return UpdatedResults wrapper with iterables for all entities and relations
   * and string of next marker
   */
  public UpdatedResults getAllUpdated(){
    UpdatedResults updatedResults;
    currentMarker = getCurrentMarker();
    try {
      String currentMarkerRep = new ObjectMapper().writeValueAsString(currentMarker);
      updatedResults = aggUpdatedResults(currentMarkerRep, emptyRunIds,
                                         DEFAULT_QUERY, DEFAULT_QUERY);
      return updatedResults;
    } catch (IOException e){
      throw Throwables.propagate(e);
    }
  }



  /** Perform incremental extraction for the entities and relations in the
   * database that have been updated or added since the extraction indicated by the marker.
   *
   * @param markerRep String from previous getAllUpdated call
   * @return UpdatedResults wrapper with iterables for updated entities and relations
   * and string of the next marker
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
      Iterable<String> extractorQueryList = getExtractorQueryList(marker, currentMarker);
      updatedResults = aggUpdatedResults(currentMarkerRep, extractorQueryList,
          DEFAULT_QUERY, DEFAULT_QUERY); //better design?
      return updatedResults;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**Perform an incremental extraction for all entities and relations that
   * satisfy the specified queries and have been added or updated since the extraction
   * indicated by the marker.
   *
   * @param markerRep String from previous getAllUpdated call
   * @param entitiesQuery Solr query string for specifying entities
   * @param relationsQuery Solr query string for specifying relations
   * @return UpdatedResults wrapper with iterables for resulting updated entities
   * and relations and string of the next marker
   */
  public UpdatedResults getAllUpdated(String markerRep,
                                      String entitiesQuery,
                                      String relationsQuery){
    UpdatedResults updatedResults;
    TypeReference<Map<String, Integer>> typeRef =
        new TypeReference<Map<String, Integer>>(){};
    currentMarker = getCurrentMarker(); //makes getAllSources() call
    try {
      String currentMarkerRep =
          new ObjectMapper().writeValueAsString(currentMarker);
      Map<String, Integer> marker =
          new ObjectMapper().readValue(markerRep, typeRef);
      Iterable<String> extractorQuery = getExtractorQueryList(marker, currentMarker);
      updatedResults = aggUpdatedResults(currentMarkerRep, extractorQuery,
                                          entitiesQuery, relationsQuery);
      return updatedResults;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /** Generate marker from each source and its sourceExtractIteration that
   * can be used to form extractorRunIds
   *
   * @return Map of sourceId to its to extractIteration
   */
  private Map<String, Integer> getCurrentMarker(){
    Collection<Source> sources = client.getAllSources();
    HashMap<String, Integer> newMarker = Maps. newHashMap();
    for (Source source : sources){
      //Source types without source IDs or extractorRunIds are unsupported
      List<SourceType> unsupportedTypes = Lists.newArrayList(SourceType.PIG,
          SourceType.IMPALA, SourceType.SPARK, SourceType.SQOOP);
      if(unsupportedTypes.contains(source.getSourceType())){ continue; }
      String id = source.getIdentity();
      Integer sourceExtractIteration = source.getSourceExtractIteration();
      newMarker.put(id, sourceExtractIteration);
    }
    return newMarker;
  }


  /** Returns an iterable of all possible extractorRunIds in between the extraction
   * states specified by marker m1 and marker m2.
   *
   * @param m1 Marker for past extraction state
   * @param m2 Marker for later(current) extraction state
   * @return Iterable of possible extractorRunIds to be used in queries
   */
  private Iterable<String> getExtractorQueryList(Map<String, Integer> m1,
                                         Map<String, Integer> m2){
    List<String> runIdList= Lists.newArrayList();
    for (String key: m1.keySet()){
      for (int i=m1.get(key); i<(m2.get(key)+1); i++){
        String possible = key + "##" + Integer.toString(i);
        runIdList.add(possible);
      }
    }
    return runIdList;
  }

  /** Constructs an UpdatedResults object with results of getAllPages
   * for entities and relations, and the marker used to generate these results.
   *
   * @param markerRep String marker from previous getAllUpdated call
   * @param extractorRunIds List of possible extractorRunIds
   * @param entitiesQuery Query string for filtering entities to extract
   * @param relationsQuery Query string filtering relations to extract
   * @return
   *
   */
  @VisibleForTesting
  public UpdatedResults aggUpdatedResults(String markerRep,
                                          Iterable<String> extractorRunIds,
                                          String entitiesQuery,
                                          String relationsQuery){
    UpdatedResults updatedResults;
    IncrementalExtractIterable<Map<String, Object>> entities =
        new IncrementalExtractIterable<>(this, "entities", entitiesQuery,
                                          100, extractorRunIds);
    IncrementalExtractIterable<Map<String, Object>> relations =
        new IncrementalExtractIterable<>(this, "relations", relationsQuery,
                                          100, extractorRunIds);
    updatedResults = new UpdatedResults(markerRep, entities, relations);
    return updatedResults;
  }

  /** Constructs url from a type (entity or relation), query, and cursorMark.
   *  Returns a batch of results that satisfy the query, starting from
   *  the cursorMark. Called in next() of IncrementalExtractIterator()
   *
   * @param type "entities" ,"relations"
   * @param queryString Solr query string
   * @return ResultsBatch set of results that satisfy query and next cursor
   */
  protected <T> ResultsBatch<T> getResultsBatch(String type,
                                      String queryString,
                                      String cursorMark){

    String fullUrlPost = ClientUtils.getUrl(config, type);
    Map<String, String> body = Maps.newHashMap();
    body.put("query", queryString);
    body.put("cursorMark", cursorMark);
    ResultsBatch<T> response = navResponse(fullUrlPost, body);
    return response;
  }

  /** Constructs a  POST Request from the given URL and body and returns the
   * response body contains a batch of results.
   *
   * @return ResultsBatch of entities or relations that specify the
   * query parameters in the URL and request body
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

  /** Writes the marker for the current state of the sources as a string
   *
   * @return String representation of a marker
   */
  @VisibleForTesting
  public String getMarker(){
    Map<String, Integer> currentMarker = getCurrentMarker();
    try {
      String rep = new ObjectMapper().writeValueAsString(currentMarker);
      return rep;
    } catch (IOException e){
      throw Throwables.propagate(e);
    }
  }

  /**Getter method for NavApiCient
   *
   * @return NavApiClient client used by these methods
   */
  public NavApiCient getClient(){ return client; }
}
