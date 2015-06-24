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

import java.util.List;

/** Wrapper class for a batch of results from a Solr query. Contains a list of
 * results and a cursor string that can be used to retrieve the next batch.
 *
 */
public class ResultsBatch<T>  {
  private String cursorMark;
  private List<T> results;

  public List<T> getResults() { return results; }

  public void setResults(List<T> results)  { this.results = results; }

  public String getCursorMark(){ return cursorMark;  }

  public void setCursorMark(String cursorMark) { this.cursorMark = cursorMark; }

}
