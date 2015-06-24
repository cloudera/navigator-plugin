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
import com.google.common.base.Throwables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

/** Example script for testing incremental extraction from a marker. Includes
 * sample marker for string formatting, and option to read/write marker to a
 * file. Log output shows total number of entities and relations extracted, and
 * how many pages/batches of results were iterated through.
 *
 * Created by Nadia.Wallace on 6/12/15.
 */
public class IncrementalUpdateExample {
  public static String  mostRecentMarker;
  public static String markerFilePath="./testMarker.txt";

  public static void main(String[] args) {
    // setup the plugin and api client
    String configFilePath = args[0];
    PluginConfigurations config = (new PluginConfigurationFactory())
        .readConfigurations(configFilePath);
    //NavigatorPlugin plugin = new NavigatorPlugin(config);
    NavApiCient client = new NavApiCient(config);

    String marker = "{\"4fb48492c18e21ae5be9c2f7faeffe62\":600," +
                      "\"4fbdadc6899638782fc8cb626176dc7b\":600," +
                      "\"efd3b52ca1bebb19990a0a92c7ff6b89\":600," +
                      "\"a063e69e6c0660353dc378c836837935\":600," +
                      "\"a09b0233cc58ff7d601eaa68673a20c6\":600}";

//    String markerFromFile="";
//    try {
//      FileReader fr = new FileReader(markerFilePath);
//      BufferedReader markerReader = new BufferedReader(fr);
//      while(markerReader.readLine()!=null){
//        markerFromFile+=markerReader.readLine();
//      }
//      markerReader.close();
//      fr.close();
//    } catch(IOException e){
//      throw Throwables.propagate(e);
//    }

    IncrementalExtractionSample ies = new IncrementalExtractionSample(client);

    UpdatedResults rs = ies.getAllUpdated(marker);
    //UpdatedResults rs = ies.getAllUpdated(markerFromFile);
    String nextMarker = rs.getMarker();
//    try {
//      PrintWriter markerWriter = new PrintWriter(markerFilePath, "UTF-8");
//      markerWriter.println(nextMarker);
//    } catch(IOException e) {
//      throw Throwables.propagate(e);
//    }

    IncrementalExtractIterable<Map<String, Object>> en = rs.getEntities();
    IncrementalExtractIterator<Map<String, Object>> entitiesIterator = en.iterator();
    Integer totalEntities = 0;
    while(entitiesIterator.hasNext()){
      Map<String,Object> nextResult = entitiesIterator.next();
      //Data processing with nextResult
      totalEntities++;
    }
    IncrementalExtractIterable<Map<String, Object>> rel = rs.getRelations();
    IncrementalExtractIterator<Map<String, Object>> relationsIterator = rel.iterator();
    Integer totalRelations = 0;
    while(relationsIterator.hasNext()){
      Map<String,Object> nextResult = relationsIterator.next();
      //Data processing with nextResult
      totalRelations++;
    }

    client.getLogger().info("Total number of entities: " + totalEntities);
    client.getLogger().info("Num entities batches: " + entitiesIterator.getNumBatchesFetched());
    client.getLogger().info("Total number of relations: " + totalRelations);
    client.getLogger().info("Num relations batches: " + relationsIterator.getNumBatchesFetched());
    client.getLogger().info("Next Marker: " + nextMarker);
  }
}
