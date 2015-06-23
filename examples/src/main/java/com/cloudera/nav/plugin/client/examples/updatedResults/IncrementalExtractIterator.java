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


import com.cloudera.nav.plugin.client.ClientUtils;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

/**
 * Created by Nadia.Wallace on 6/19/15.
 */
public class IncrementalExtractIterator<T> implements Iterator<T> {

  private final IncrementalExtractionSample ies;
  private final String type;
  private final String userQuery;
  private String fullQuery;
  private final Integer limit;
  private final Integer MAX_QUERY_PARTITION_SIZE = 800;
  private Iterable<List<String>> partitionedRunIds;
  private boolean hasNext;
  private String cursorMark="*";
  private String nextCursorMark;
  private List<T> results;
  private Integer resultIndex = 0;
  private Integer partitionIndex = 0;
  private Integer numBatchesFetched = 0;


  public IncrementalExtractIterator(IncrementalExtractionSample ies,
                                    String type, String query, Integer limit,
                                    Iterable<String> extractorRunIds){
    this.ies = ies;
    this.type = type;
    this.userQuery = query;
    this.limit = limit;
    this.partitionedRunIds = Iterables.partition(extractorRunIds, MAX_QUERY_PARTITION_SIZE);
    if(!Iterables.isEmpty(extractorRunIds)) {
      updateFullQuery(userQuery, partitionedRunIds.iterator().next());
    } else {
      fullQuery = userQuery;
    }
    getNextDocs(type);
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public T next() {
    if(!hasNext()){
      throw new NoSuchElementException();
    }
    T nextResult = results.get(resultIndex);
    resultIndex++;
    Iterator<List<String>> partitionIterator = partitionedRunIds.iterator();
    //if at last element in docs
    if(resultIndex == results.size()){
        //if on last batch
        if (results.size() < limit) {
          //if on last query
          if(!partitionIterator.hasNext()) {
            hasNext = false; //leave loop
          //else get next query
          } else {
            updateFullQuery(userQuery, partitionIterator.next());
            getNextDocs(type); //placement?
          }
        //else fetch next batch
        } else {
          cursorMark = nextCursorMark;
          nextCursorMark = null;
          getNextDocs(type);
        }
       //adds more results, resets resultIndex, updates hasNext + nextCursorMark
      numBatchesFetched++;
    }
    return nextResult;
  }

  public void getNextDocs(String type){
    ResultsBatch<T> response = ies.getResultsBatch(type, fullQuery, cursorMark);
    results = response.getResults();
    nextCursorMark = response.getCursorMark();
    hasNext = results.size() > 0;
    resultIndex = 0; //start from beginning
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public Integer getNumBatchesFetched(){ return numBatchesFetched; }

  private void updateFullQuery(String userQuery, List<String> extractorRunIds){
    String extractorString = ClientUtils.buildConjunctiveClause(extractorRunIds);
    fullQuery = ClientUtils.conjoinSolrQueries(userQuery, extractorString);
  }
}
