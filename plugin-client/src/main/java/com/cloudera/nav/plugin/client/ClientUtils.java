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

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.http.HttpHeaders;

/** A set of functions for forming and handling URLs and  Solr queries.
 *
 */
public class ClientUtils {

  /**Form headers for sending API calls to the Navigator server
   *
   * @param config plugin configuration to get authorization info from
   * @return HttpHeaders headers for authorizing the plugin
   */
  public static HttpHeaders getAuthHeaders(PluginConfigurations config) {
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

  /**Get url for querying all sources
   *
   * @return url for querying all sources
   */
  public static String getSourceUrl(PluginConfigurations config, String query) {
    // form the url string to request all entities with type equal to SOURCE
    String baseNavigatorUrl = config.getNavigatorUrl();
    String entitiesUrl = joinUrlPath(baseNavigatorUrl, "entities");
    return String.format("%s?query=%s", entitiesUrl, query);
    //return String.format("%s?query=%s", entities, SOURCE_QUERY);
  }

  /** Get URL for incremental extraction
   *
   * @param type "entities", "relations"
   * @return url for querying entities and relations
   */
  public static String getUrl(PluginConfigurations config, String type) {
    String baseNavigatorUrl = config.getNavigatorUrl();
    String typeUrl = joinUrlPath(baseNavigatorUrl, type);
    if(type.equals("entities")) {
      return typeUrl+"/paging";
    } else { return typeUrl;}
  }

  /** Joining two URL components properly by handling slashes
   *
   * @param base First part of the URL
   * @param component Second part of the URL
   * @return String url of base/component
   */
  private static String joinUrlPath(String base, String component) {
    return base + (base.endsWith("/") ? "" : "/") + component;
  }

  /** Makes a conjunctive "AND" Solr query with two clauses.
   *
   * @param q1 Solr query string
   * @param q2 Solr query string
   * @return (q1) AND (q2)
   */
  public static String conjoinSolrQueries(String q1, String q2){
    if(q1.isEmpty()){ return q2; }
    if(q2.isEmpty()){ return q1; }
    return "(" + q1 + ") AND (" + q2 + ")";
  }

  /** Constructs a single conjunctive clause of values with "OR"
   *
   * @param fieldName field name that clause values can satisfy
   * @param values iterable of possible values
   * @return conjunctive clause string as fieldName:(a OR b ...)
   */
  public static String buildConjunctiveClause(String fieldName,
                                              Iterable<String> values){
    String orQuery = fieldName+":(";
    Iterator<String> valuesIterator = values.iterator();
    while(valuesIterator.hasNext()){
      String value = valuesIterator.next();
      orQuery = orQuery + value + " OR ";
    }
    return orQuery.substring(0, orQuery.length()-4)+")";
  }

  /** Convert desired values to valid query string in solr syntax in
   * conjunctive normal form as sourceType:(i OR j ...) AND type:(a OR b ...)
   *
   * @param sourceTypes collection of sourceType field values
   * @param types collection of type field values
   * @return Conjunctive clauses of sourceTypes and types
   */
  public static String queryBuilder(Collection<String> sourceTypes,
                             Collection<String> types){
    String sourceClause = buildConjunctiveClause("sourceType", sourceTypes);
    String typeClause = buildConjunctiveClause("type", types);
    String query = conjoinSolrQueries(sourceClause, typeClause);
    return query;
  }
}
