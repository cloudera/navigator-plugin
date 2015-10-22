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

import com.google.common.base.Joiner;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;

/**
 * A set of functions for forming Solr queries.
 */
public class QueryUtils {

  /**
   * Makes a conjunctive "AND" Solr query with two clauses.
   *
   * @param q1 Solr query string
   * @param q2 Solr query string
   * @return (q1) AND (q2)
   */
  public static String conjoinSolrQueries(String q1, String q2){
    if(StringUtils.isEmpty(q1)){
      return q2;
    }
    if(StringUtils.isEmpty(q2)){
      return q1;
    }
    return q1 + " AND " + q2 ;
  }

  /**
   * Constructs a single conjunctive clause of values with "OR"
   *
   * @param fieldName field name that clause values can satisfy
   * @param values iterable of possible values
   * @return conjunctive clause string as fieldName:(a OR b ...)
   */
  public static String buildConjunctiveClause(String fieldName,
                                              Collection<String> values){
    Joiner conjunctiveJoiner = Joiner.on(" OR ").skipNulls();
    return fieldName + ":(" + conjunctiveJoiner.join(values) + ")";
  }

  /**
   * Convert desired values to valid query string in solr syntax in
   * conjunctive normal form as sourceType:(i OR j ...) AND type:(a OR b ...)
   *
   * @param sourceTypes collection of sourceType field values
   * @param types collection of type field values
   * @return Conjunctive clauses of sourceTypes and types
   */
  public static String buildQuery(Collection<String> sourceTypes,
                                  Collection<String> types){
    String sourceClause = buildConjunctiveClause("sourceType", sourceTypes);
    String typeClause = buildConjunctiveClause("type", types);
    return conjoinSolrQueries(sourceClause, typeClause);
  }
}
