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
package com.cloudera.nav.sdk.client;

import com.cloudera.com.fasterxml.jackson.core.type.TypeReference;
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import com.cloudera.nav.sdk.model.MetadataType;
import com.cloudera.nav.sdk.model.Source;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * A simple example class to extract entities and relations via the overloaded
 * extractMetadata method. Calling extractMetadata with no arguments is a bulk
 * extraction of all metadata in Navigator at request time. The returned
 * MetadataResultSet contains a string marker than can be used to mark the
 * starting point for the next time extractMetadata is called. When given a
 * single marker, metadata since that marker is extracted. When given 2 markers
 * metadata extracted between the 2 markers are returned.
 *
 * The marker isn't designed to be used public API. Under the hood it is
 * composed of extractorRunId's that is incremented by the server every time
 * it reads additional metadata from Hadoop services.
 */
public class MetadataExtractor {

  private NavApiCient client;
  private static final String DEFAULT_QUERY= "identity:*";
  private static final Integer DEFAULT_LIMIT = 100;
  private final Integer limit;

  public MetadataExtractor(NavApiCient client, Integer limit) {
    this.client = client;
    this.limit = (limit == null) ? DEFAULT_LIMIT : limit;
  }

  /**
   * Returns all of the entities and relations in Navigator,
   * plus a marker to denote when this search took place
   */
  public MetadataResultSet extractMetadata() {
    return extractMetadata(null, null, DEFAULT_QUERY, DEFAULT_QUERY);
  }

  /**
   * Perform incremental extraction for the entities and relations in
   * Navigator that have been updated or added since the extraction indicated
   * by the marker.
   *
   * @param markerRep marker from previous extractMetadata call
   */
  public MetadataResultSet extractMetadata(String markerRep) {
    return extractMetadata(markerRep, null, DEFAULT_QUERY, DEFAULT_QUERY);
  }

  /**
   * Perform incremental extraction for the entities and relations in
   * Navigator that have been updated or added between the start and end markers
   *
   * @param startMarker
   * @param endMarker
   */
  public MetadataResultSet extractMetadata(String startMarker,
                                           String endMarker) {
    return extractMetadata(startMarker, endMarker, DEFAULT_QUERY,
        DEFAULT_QUERY);
  }

  /**
   * Perform an incremental extraction for all entities and relations that
   * satisfy the specified queries and have been added or updated since the extraction
   * indicated by the marker.
   *
   * @param startMarkerRep String from previous extractMetadata call
   * @param entitiesQuery Solr query string for specifying entities
   * @param relationsQuery Solr query string for specifying relations
   * @return MetadataResultSet wrapper with iterables for resulting updated entities
   * and relations and string of the next marker
   */
  public MetadataResultSet extractMetadata(String startMarkerRep,
                                           String endMarkerRep,
                                           String entitiesQuery,
                                           String relationsQuery) {
    try {
      TypeReference<Map<String, Long>> typeRef =
          new TypeReference<Map<String, Long>>(){};
      Iterable<String> extractorQuery;
      Map<String, Long> endMarker;
      if(StringUtils.isEmpty(startMarkerRep) && StringUtils.isEmpty(endMarkerRep)){
        extractorQuery = Lists.newArrayList("*");
      } else {
        Map<String, Long> startMarker;
        if(StringUtils.isEmpty(startMarkerRep)) {
          startMarker = getNavMarker(false);
        } else {
          startMarker = new ObjectMapper().readValue(startMarkerRep, typeRef);
        }
        if(StringUtils.isEmpty(endMarkerRep)) {
          endMarker = getNavMarker(true);
        } else {
          endMarker = new ObjectMapper().readValue(endMarkerRep, typeRef);
        }
        extractorQuery = getExtractorQueryList(startMarker, endMarker);
      }
      String currentMarkerRep = new ObjectMapper().writeValueAsString(getNavMarker(true));
      return aggUpdatedResults(currentMarkerRep, extractorQuery, entitiesQuery, relationsQuery);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Generate marker from each source and its sourceExtractIteration that
   * can be used to form extractorRunIds
   *
   * @return Map of sourceId to its to extractIteration
   */
  private Map<String, Long> getNavMarker(boolean current) {
    Collection<Source> sources = client.getAllSources();
    HashMap<String, Long> newMarker = Maps. newHashMap();
    for (Source source : sources) {
      String id = source.getIdentity();
      if (source.getSourceExtractIteration() == null) {
        continue;
      }
      Long sourceExtractIteration = (current) ?
          source.getSourceExtractIteration() : 0;
      newMarker.put(id, sourceExtractIteration);
    }
    return newMarker;
  }

  /**
   * Returns an iterable of all possible extractorRunIds in between the extraction
   * states specified by marker m1 and marker m2.
   *
   * @param m1 Marker for past extraction state
   * @param m2 Marker for later(current) extraction state
   * @return Iterable of possible extractorRunIds to be used in queries
   */
  private Iterable<String> getExtractorQueryList(Map<String, Long> m1,
                                                 Map<String, Long> m2) {
    List<String> runIdList= Lists.newArrayList();
    for (String key: m1.keySet()) {
      for (long i=m1.get(key); i<(m2.get(key)+1); i++) {
        String possible = key + "##" + Long.toString(i);
        runIdList.add(possible);
      }
    }
    return runIdList;
  }

  /**
   * Constructs an MetadataResultSet object with results of getAllPages
   * for entities and relations, and the marker used to generate these results.
   *
   * @param markerRep String marker from previous extractMetadata call
   * @param extractorRunIds List of possible extractorRunIds
   * @param entitiesQuery Query string for filtering entities to extract
   * @param relationsQuery Query string filtering relations to extract
   * @return MetadataResultSet with resulting entities, relations and marker
   */
  private MetadataResultSet aggUpdatedResults(String markerRep,
                                          Iterable<String> extractorRunIds,
                                          String entitiesQuery,
                                          String relationsQuery) {
    MetadataResultSet metadataResultSet;
    MetadataIterable entities = new MetadataIterable(client,
        MetadataType.ENTITIES, entitiesQuery, limit, extractorRunIds);
    MetadataIterable relations = new MetadataIterable(client,
        MetadataType.RELATIONS, relationsQuery, limit, extractorRunIds);
    metadataResultSet = new MetadataResultSet(markerRep, entities, relations);
    return metadataResultSet;
  }

  /**
   *  Writes the marker for the current state of the sources as a string
   *
   * @return String representation of a marker
   */
  public String getMarker() {
    Map<String, Long> currentMarker = getNavMarker(true);
    try {
      return new ObjectMapper().writeValueAsString(currentMarker);
    } catch (IOException e){
      throw Throwables.propagate(e);
    }
  }
}