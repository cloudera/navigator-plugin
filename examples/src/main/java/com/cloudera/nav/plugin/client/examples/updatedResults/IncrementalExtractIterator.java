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


import com.cloudera.nav.plugin.client.ResultsBatch;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map;
import java.util.List;

/**
 * Created by Nadia.Wallace on 6/19/15.
 */
public class IncrementalExtractIterator<T> implements Iterator<T> {

  private final IncrementalExtractionSample ies;
  private final String type;
  private final String query;
  private final Integer limit;
  private boolean hasNext;
  private String cursorMark="*";
  private String nextCursorMark;
  private List<T> results;
  private Integer resultIndex = 0;
  private Integer numBatchesFetched = 0;

  public IncrementalExtractIterator(IncrementalExtractionSample ies,
                                    String type, String query, Integer limit){
    this.ies = ies;
    this.type = type;
    this.query = query;
    this.limit = limit;
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
    if(resultIndex == results.size()){ //if at last element in docs
      if(results.size() < limit){ //if on last batch
        hasNext = false; //leave loop
      } else { //else fetch next batch
        cursorMark = nextCursorMark;
        nextCursorMark = null;
        getNextDocs(type); //adds more results, resets resultIndex, updates hasNext + nextCursorMark
        numBatchesFetched++;
      }
    }
    return nextResult; //Ask Chung about iterating one by one
  }

  public void getNextDocs(String type){
    ResultsBatch<T> response = ies.getResultsBatch(type, query, cursorMark);
    results = response.getResults();
    nextCursorMark = response.getCursorMark();
    hasNext = results.size() > 0;
    resultIndex = 0; //start from beginning
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public Integer getNumBatchesFetched(){ return this.getNumBatchesFetched(); }
}
