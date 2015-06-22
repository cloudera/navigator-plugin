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
import com.cloudera.nav.plugin.client.UpdatedResults;
import com.google.common.collect.Iterables;

import java.util.Map;

/**Example for using getAllUpdated() methods to retrieve all entities and
 * relations modified since a given set of extractions. Intended as initial
 * bulk extraction to generate the first marker.
 *
 * Created by Nadia.Wallace on 6/4/15.
 */
public class GetAllUpdatedResults {

  private static String firstMarker;
  private static String  incrementMarker;

  public static String getFirstMarker() { return firstMarker; }

  public static String getIncrementMarker(){ return incrementMarker; }

  public static void main(String[] args){
    // setup the plugin and api client
    String configFilePath = args[0];
    PluginConfigurations config = (new PluginConfigurationFactory())
            .readConfigurations(configFilePath);
    //NavigatorPlugin plugin = new NavigatorPlugin(config);
    NavApiCient client = new NavApiCient(config);

    IncrementalExtractionSample ies = new IncrementalExtractionSample(client);
    String current = ies.getMarker();
    System.out.println(current);

    UpdatedResults resultsNoMarker = ies.getAllUpdated();
    String marker1 = resultsNoMarker.getMarker();
    firstMarker = marker1; //write to a file
    System.out.println(firstMarker); //use JUnit assertions - expect Map

    Iterable<Map<String, Object>> entities1 = resultsNoMarker.getEntities();
    System.out.println("Number of entities: " + Iterables.size(entities1));
    Iterable<Map<String, Object>> relations1 = resultsNoMarker.getRelations();
    System.out.println("\n Number of relations: " + Iterables.size(relations1));


  }
}
