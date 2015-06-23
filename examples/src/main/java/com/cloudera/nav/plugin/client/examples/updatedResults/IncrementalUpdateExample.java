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

import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;

import java.util.Iterator;
import java.util.Map;

/** Example script for testing incremental extraction from a marker
 *
 * Created by Nadia.Wallace on 6/12/15.
 */
public class IncrementalUpdateExample {
  public static String  mostRecentMarker;

  public static void main(String[] args) {
    // setup the plugin and api client
    String configFilePath = args[0];
    PluginConfigurations config = (new PluginConfigurationFactory())
        .readConfigurations(configFilePath);
    //NavigatorPlugin plugin = new NavigatorPlugin(config);
    NavApiCient client = new NavApiCient(config);

    String marker = "{\"4fb48492c18e21ae5be9c2f7faeffe62\":560," +
                      "\"4fbdadc6899638782fc8cb626176dc7b\":560," +
                      "\"efd3b52ca1bebb19990a0a92c7ff6b89\":560," +
                      "\"a063e69e6c0660353dc378c836837935\":560," +
                      "\"a09b0233cc58ff7d601eaa68673a20c6\":560}";

    UpdatedResults incrementResults;
    IncrementalExtractionSample ies = new IncrementalExtractionSample(client);

    UpdatedResults rs = ies.getAllUpdated(marker);
    String nextMarker = rs.getMarker();

    IncrementalExtractIterable<Map<String, Object>> en = rs.getEntities();
    IncrementalExtractIterator<Map<String, Object>> entitiesIterator = en.iterator();
    Integer totalEntities = 0;
    while(entitiesIterator.hasNext()){
      Map<String,Object> nextResult = entitiesIterator.next();
      totalEntities++;
    }
    IncrementalExtractIterable<Map<String, Object>> rel = rs.getRelations();
    IncrementalExtractIterator<Map<String, Object>> relationsIterator = rel.iterator();
    Integer totalRelations = 0;
    while(relationsIterator.hasNext()){
      Map<String,Object> nextResult = relationsIterator.next();
      totalRelations++;
    }

    System.out.println("Total number of entities: " + totalEntities);
    System.out.println("Num entities batches: " + entitiesIterator.getNumBatchesFetched());
    System.out.println("Total number of relations: " + totalRelations);
    System.out.println("Num relations batches: " + relationsIterator.getNumBatchesFetched());
    System.out.println("Next Marker: " + nextMarker);
  }
}
