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
package com.cloudera.nav.plugin.client;


import com.cloudera.com.fasterxml.jackson.core.type.TypeReference;
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * An API client to communicate with Navigator to register and validate
 * metadata models
 */
public class NavApiCient {

  private static final Logger LOG = LoggerFactory.getLogger(NavApiCient.class);
  private static final String SOURCE_QUERY = "type:SOURCE";
  private static final String ALL_QUERY = "type:*";

  private final PluginConfigurations config;
  private final Cache<String, Source> sourceCacheByUrl;
  private final Cache<SourceType, Collection<Source>> sourceCacheByType;

  public NavApiCient(PluginConfigurations config) {
    this.config = config;
    sourceCacheByUrl = CacheBuilder.newBuilder().build();
    sourceCacheByType = CacheBuilder.newBuilder().build();
  }

  /**
   * Registers a given set of metadata models
   * @param models
   */
  public void registerModels(Collection<Object> models) {
    throw new UnsupportedOperationException("no yet implemented");
  }

  /**
   * Call the Navigator API and retrieve all available sources
   *
   * @return a collection of available sources
   */
  public Collection<Source> getAllSources() {
//    System.out.println("here");
    RestTemplate restTemplate = new RestTemplate();
    String url = getUrl();
    HttpHeaders headers = getAuthHeaders();
//    System.out.println(headers);
    HttpEntity<String> request = new HttpEntity<String>(headers);
    Class<SourceAttrs[]> sourceAttrsClass = SourceAttrs[].class;
    ResponseEntity<SourceAttrs[]> response = restTemplate.exchange(url,
        HttpMethod.GET, request, sourceAttrsClass);
//    ResponseEntity<SourceAttrs[]> response = restTemplate.getForEntity(url, )
//    System.out.println(response);
    Collection<Source> sources = Lists.newArrayList();
    for (SourceAttrs info : response.getBody()) {
      sources.add(info.createSource());
    }
    return sources;
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

  public UpdatedResults aggUpdatedResults(String markerRep, String queryString){
    UpdatedResults updatedResults;
    ParameterizedTypeReference<ResultsBatch> resultClass = new ParameterizedTypeReference<ResultsBatch>(){};
    List<Map<String, Object>> entities = getAllPages("entities", queryString, resultClass);
    List<Map<String, Object>> relations = getAllPages("relations", queryString, resultClass);
    updatedResults = new UpdatedResults(markerRep, entities, relations);
    return updatedResults;
  }

  public List<Map<String, Object>> getAllPages(String type, String queryString, ParameterizedTypeReference<ResultsBatch> resultClass){
    List<Map<String, Object>> result = new ArrayList<>();
    boolean done = false;
    String cursorMark="*";
    while (!done){
      UriComponentsBuilder uri = UriComponentsBuilder.fromHttpUrl(getUrl(type)).query(queryString);
      uri.replaceQueryParam("cursorMark", cursorMark);
      uri.fragment(null);
      URI fullUrl = uri.build().toUri();
      ResultsBatch response = navResponse(type, fullUrl, resultClass);
      result.addAll(Arrays.asList(response.getResults()));
      String newCursorMark = response.getCursorMark();
      if(newCursorMark.equals(cursorMark)){
        done=true;
      }
      cursorMark = newCursorMark;
    }
    return result;
  }

  /**
   *extractorRunId:("4fbdadc6899638782fc8cb626176dc7b##1253" OR "a09b0233cc58ff7d601eaa68673a20c6##1248" OR
   * @param type
   * @param resultClass
   * @param <T>
   * @return
   */
  //This method has the restTemplate calls --> mock it
  public <T> T navResponse(String type, URI url, ParameterizedTypeReference<T> resultClass){
    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = getAuthHeaders();
    HttpEntity<String> request =new HttpEntity<String>(headers);
    ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.GET, request, resultClass);
    T responseResult = response.getBody();
    return responseResult;
  }

  /**
   *
   * @return
   */
  public Map<String, Integer> getCurrentMarker(){
    Collection<Source> sources = getAllSources(); //loadAllSources/cache?
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

  /**
   * Get the Source corresponding to the Hadoop service Url from Navigator.
   * A NoSuchElementException is thrown if the url does not correspond to
   * any known Source
   *
   * @param serviceUrl
   * @return
   */
  public Source getSourceForUrl(String serviceUrl) {
    Source source = sourceCacheByUrl.getIfPresent(serviceUrl);
    if (source == null) {
      loadAllSources();
    }
    source = sourceCacheByUrl.getIfPresent(serviceUrl);
    Preconditions.checkArgument(source != null,
        "Could not find Source at " + serviceUrl);
    return source;
  }

  /**
   * Return the only Source of the given type, throw exception if more than
   * one is found.
   *
   * @param sourceType
   * @return
   */
  public Source getOnlySource(SourceType sourceType) {
    Collection<Source> sources = getSourcesForType(sourceType);
    Preconditions.checkNotNull(sources, "Could not find sources for " +
        "source type " + sourceType.name());
    return Iterables.getOnlyElement(sources);
  }

  public Collection<Source> getSourcesForType(SourceType sourceType) {
    Collection<Source> sources = sourceCacheByType.getIfPresent(sourceType);
    if (sources == null) {
      loadAllSources();
      sources = sourceCacheByType.getIfPresent(sourceType);
    }
    return sources;
  }

  /**
   * Clear the cache of Sources that have been previously loaded.
   */
  public void resetSources() {
    sourceCacheByUrl.invalidateAll();
    sourceCacheByType.invalidateAll();
  }

  private HttpHeaders getAuthHeaders() {
    // basic authentication with base64 encoding
    String plainCreds = String.format("%s:%s", config.getUsername(),
        config.getPassword());
    byte[] plainCredsBytes = plainCreds.getBytes();
    byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
    String base64Creds = new String(base64CredsBytes);
    HttpHeaders headers = new HttpHeaders();
    headers.add("Authorization", "Basic " + base64Creds);
    return headers;
  }

  /**
   *
   * @return url for querying all sources
   */
  private String getUrl() {
    // form the url string to request all entities with type equal to SOURCE
    String baseNavigatorUrl = config.getNavigatorUrl();
    String entities = joinUrlPath(baseNavigatorUrl, "entities");
    return String.format("%s?query=%s%s", entities, SOURCE_QUERY, "&debugQuery=on");
    //return String.format("%s?query=%s", entities, SOURCE_QUERY);
  }

  /**
   * @param type "entities", "relations"
   * @return url for querying entities and relations
   */
  private String getUrl(String type) {
    String baseNavigatorUrl = config.getNavigatorUrl();
    String typeUrl = joinUrlPath(baseNavigatorUrl, type);
    if(type.equals("entities")) {
      return typeUrl+"/paging";
    } else { return typeUrl;}
  }

  private String joinUrlPath(String base, String component) {
    return base + (base.endsWith("/") ? "" : "/") + component;
  }

  private void loadAllSources() {
    Collection<Source> allSources = getAllSources();
    for (Source source : allSources) {
      if (source.getSourceUrl() == null) {
        LOG.warn(String.format("Source %s did not have a source url",
            source.getName() != null ? source.getName() :
                source.getIdentity()));
        continue;
      }
      sourceCacheByUrl.put(source.getSourceUrl(), source);
      try {
        Collection<Source> forType = sourceCacheByType.get(
            source.getSourceType(),
            new Callable<Collection<Source>>() {
              @Override
              public Collection<Source> call() throws Exception {
                return Sets.newHashSet();
              }
            });
        forType.add(source);
      } catch (ExecutionException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
