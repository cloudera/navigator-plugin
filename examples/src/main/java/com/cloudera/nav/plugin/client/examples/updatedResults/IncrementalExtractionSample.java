package com.cloudera.nav.plugin.client.examples.updatedResults;

import com.cloudera.com.fasterxml.jackson.core.type.TypeReference;
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import com.cloudera.nav.plugin.client.ClientUtils;
import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.client.ResultsBatch;
import com.cloudera.nav.plugin.client.UpdatedResults;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Created by Nadia.Wallace on 6/16/15.
 */
public class IncrementalExtractionSample {

  private NavApiCient client;
  private PluginConfigurations config;

  public IncrementalExtractionSample(NavApiCient client){
    this.client = client;
    this.config = client.getConfig();
  }

  /** Returns all of the entities and relations in the database, plus a marker to denote when this search took place
   *
   * @return
   */
  public UpdatedResults getAllUpdated(){
    UpdatedResults updatedResults;
    Map<String, Integer> currentMarker = getCurrentMarker();
    try {
      String currentMarkerRep = new ObjectMapper().writeValueAsString(currentMarker);
      String queryString = "query=identity:*";
      updatedResults = aggUpdatedResults(currentMarkerRep, queryString);
      return updatedResults;
    } catch (IOException e){
//      System.err.println(e.getMessage());
      throw Throwables.propagate(e);
    }
  }



  /**Returns all of the entities and relations in the database that have been updated or added since the source iterations indicated by the marker
   *
   * @param markerRep JSON representation of sourceId : sourceExtractIteration
   * @return
   */

  public UpdatedResults getAllUpdated(String markerRep){
    UpdatedResults updatedResults;
    Map<String, Integer> currentMarker = getCurrentMarker();
    try {
      String currentMarkerRep = new ObjectMapper().writeValueAsString(currentMarker);
      Map<String, Integer> marker = new ObjectMapper().readValue(markerRep, new TypeReference<Map<String, Integer>>(){});
      String extractorQueryString = getExtractorQueryString(marker, currentMarker);
      updatedResults = aggUpdatedResults(currentMarkerRep, extractorQueryString);
      return updatedResults;
    } catch (IOException e) {
      System.err.println(e.getMessage());
      throw Throwables.propagate(e);
    }
  }

  /** Constructs a query in Solr syntax as "<extractorRunId1> OR <extractorRunId2> ..." for all extraction iterations per source between m1 and m2
   *
   * @param m1
   * @param m2
   * @return
   */
  private String getExtractorQueryString(Map<String, Integer> m1, Map<String, Integer> m2){
    String queryString = "query=extractorRunId:(";
    for (String key: m1.keySet()){
      for (int i=m1.get(key); i<(m2.get(key)+1); i++){
        String possible = key + "##" + Integer.toString(i);
        queryString = queryString + possible + " OR ";
      }
    }
    return queryString.substring(0, queryString.length()-4)+")";
  }

  /** Constructs an UpdatedResults object with results of getAllPages for entities and relations, and the marker used to generate these results.
   *
   * @param markerRep
   * @param queryString
   * @return
   */
  private UpdatedResults aggUpdatedResults(String markerRep, String queryString){
    UpdatedResults updatedResults;
    List<Map<String, Object>> entities = getAllPages("entities", queryString);
    List<Map<String, Object>> relations = getAllPages("relations", queryString);
    updatedResults = new UpdatedResults(markerRep, entities, relations);
    return updatedResults;
  }

  /** Constructs the url from a type (entity or relation), query, and cursorMark. Iterates through cursor marks and returns all updated entities and relations.
   *
   * @param type
   * @param queryString
   * @return
   */
  private List<Map<String, Object>> getAllPages(String type, String queryString){
    String fullUrl = ClientUtils.getUrl(config, type);
    MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
    body.add("query",  queryString);

    List<Map<String, Object>> result = new ArrayList<>();
    boolean done = false;
    String cursorMark="*";
    while (!done){
//      UriComponentsBuilder uri = UriComponentsBuilder.fromHttpUrl(ClientUtils.getUrl(client.getConfig(), type)).query(queryString);
//      uri.replaceQueryParam("cursorMark", cursorMark);
//      uri.fragment(null);
//      URI fullUrl = uri.build().toUri();
      body.add("cursorMark", cursorMark);
      ResultsBatch response = navResponse(fullUrl, body);
      result.addAll(Arrays.asList(response.getResults()));
      String newCursorMark = response.getCursorMark();
      if(newCursorMark.equals(cursorMark)){
        done=true;
      }
      cursorMark = newCursorMark;
    }
    return result;
  }

  /**Makes the HTTP call from a given url to retrieve a ResultsBatch object that represents the set of elements that fit
   * the query parameters (extractorRunIds and cursorMark)
   * @param formData
   * @param url
   * @return
   */
  //This method has the restTemplate calls --> mock it
  private ResultsBatch navResponse(String url,  MultiValueMap<String,String> formData){
    ParameterizedTypeReference<ResultsBatch> resultClass = new ParameterizedTypeReference<ResultsBatch>(){};
    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = ClientUtils.getAuthHeaders(config);
    HttpEntity<MultiValueMap<String, String>> request =new HttpEntity<>(formData, headers);
    ResponseEntity<ResultsBatch> response = restTemplate.exchange(url, HttpMethod.POST, request, resultClass);
    ResultsBatch responseResult = response.getBody();
    return responseResult;
  }

  /** Generate marker from each source and its sourceExtractIteration
   *
   * @return
   */
  private Map<String, Integer> getCurrentMarker(){
    Collection<Source> sources = client.getAllSources(); //loadAllSources/cache?
    HashMap<String, Integer> newMarker = new HashMap<>();
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
}
