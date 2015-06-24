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

import java.util.Iterator;

/**Iterable used for entities and relations in UpdatedResults object returned
 * by getAllUpdated() calls.
 *
 */
public class IncrementalExtractIterable<T> implements Iterable<T> {

  private final IncrementalExtractionSample ies;
  private final String type;
  private final String query;
  private final Integer limit;
  private final Iterable<String> extractorRunIds;

  public IncrementalExtractIterable(IncrementalExtractionSample ies, String type,
                                    String query, Integer limit,
                                    Iterable<String> extractorRunIds){
    this.query = query;
    this.type = type;
    this.ies = ies;
    this.limit =limit;
    this.extractorRunIds=extractorRunIds;
  }

  @Override
  public IncrementalExtractIterator<T> iterator() {
    return new IncrementalExtractIterator<>(ies, type, query, limit, extractorRunIds);
  }
}
