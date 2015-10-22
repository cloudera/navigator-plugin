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

import com.cloudera.nav.sdk.model.MetadataType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Lazy iterator over metadata (entities or relations determined by given
 * MetadataType) that satisfies the given String query and the given
 * extractorRunIds.
 *
 * Under the hood, the iterator combines the query and the extractorRunIds and
 * sends a request via the given NavApiClient. The results are fetched in
 * batches.
 */
public class MetadataResultIterator implements Iterator<Map<String, Object>> {

  public static final Integer MAX_QUERY_PARTITION_SIZE = 800;

  private final NavApiCient client;
  private final Integer limit;
  private final MetadataType type;
  private final String userQuery;
  private boolean hasNext;
  private Iterator<List<String>> partitionRunIdIterator;
  private List<Map<String, Object>> resultsBatch;
  private Iterator<Map<String, Object>> resultsBatchIterator;
  private String cursorMark = "*";
  private String nextQuery;

  public MetadataResultIterator(NavApiCient client, MetadataType type,
                                String query, Integer limit,
                                Iterable<String> extractorRunIds) {
    this.client = client;
    this.type = type;
    this.userQuery = query;
    this.limit = limit;
    this.partitionRunIdIterator = Iterables.partition(extractorRunIds,
        MAX_QUERY_PARTITION_SIZE).iterator();
    if(Iterables.isEmpty(extractorRunIds)) {
      nextQuery = userQuery;
    } else {
      getNextQuery();
    }
    getNextBatch();
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public Map<String, Object> next() {
    if(!hasNext()) {
      throw new NoSuchElementException();
    }
    Map<String, Object> nextResult = resultsBatchIterator.next();
    //if at last element in batch
    if(!resultsBatchIterator.hasNext()){
      //if on last batch
      if(resultsBatch.size()<limit) {
        //if on last query, leave loop
        if (!partitionRunIdIterator.hasNext()) {
          hasNext = false;
        //Update query and get next batch
        } else {
          getNextQuery();
          getNextBatch();
        }
      //fetch next batch
      } else {
        getNextBatch();
      }
    }
    return nextResult;
  }

  @VisibleForTesting
  void getNextBatch() {
    // Retrieve the next batch of metadata results
    try {
      ResultsBatch<Map<String, Object>> response = getResultsBatch();
      resultsBatch = response.getResults();
      resultsBatchIterator = resultsBatch.iterator();
      hasNext = resultsBatchIterator.hasNext();
      cursorMark = response.getCursorMark();
      if (!hasNext && partitionRunIdIterator.hasNext()) {
        getNextQuery();
        getNextBatch();
      }
    } catch (Exception e){
      throw Throwables.propagate(e);
    }
  }

  private ResultsBatch<Map<String, Object>> getResultsBatch() {
    // Send the next request to the server to get a batch of results
    MetadataQuery query = new MetadataQuery(nextQuery, limit, cursorMark);
      switch(type) {
        case ENTITIES:
          return client.getEntityBatch(query);
        case RELATIONS:
          return client.getRelationBatch(query);
        default:
          throw new UnsupportedOperationException("Invalid MetadataType " +
          type.name());
      }
  }

  private void getNextQuery() {
    // create the next query by combining the given userQuery with the next
    // partition of extractorRunIds
    cursorMark="*";
    List<String> extractorRunIdBatch = partitionRunIdIterator.next();
    String extractorString = QueryUtils.buildConjunctiveClause(
        "extractorRunId", extractorRunIdBatch);
    nextQuery = QueryUtils.conjoinSolrQueries(userQuery, extractorString);
  }

  /**
   * Unsupported
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
