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

import static org.junit.Assert.*;

import com.google.common.collect.Lists;

import java.util.List;

import org.junit.*;

/**
 * Test for helper functions in ClientUtils
 */
public class QueryUtilsTest {

  @Test
  public void testSolrClause(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    String solrClause = QueryUtils.buildConjunctiveClause("extractorRunIds",
        extractorRunIds);
    String ans = "extractorRunIds:(x##0 OR x##1 OR x##2)";
    assertEquals(ans, solrClause);
  }

  @Test
  public void testSolrQuery(){
    String clause1 = "extractorRunIds:(x##0 OR x##1 OR x##2)";
    String clause2 = "identity:(foo OR bar OR baz)";
    String fullQuery = QueryUtils.conjoinSolrQueries(clause1, clause2);
    String ans = "extractorRunIds:(x##0 OR x##1 OR x##2) " +
        "AND identity:(foo OR bar OR baz)";
    assertEquals(ans, fullQuery);
  }
}
