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

    String marker = "{\"4fb48492c18e21ae5be9c2f7faeffe62\":1834," +
                      "\"4fbdadc6899638782fc8cb626176dc7b\":1832," +
                      "\"efd3b52ca1bebb19990a0a92c7ff6b89\":1833," +
                      "\"a063e69e6c0660353dc378c836837935\":1832," +
                      "\"a09b0233cc58ff7d601eaa68673a20c6\":1822}";

    UpdatedResults incrementResults;
    IncrementalExtractionSample ies = new IncrementalExtractionSample(client);
    if (mostRecentMarker != null) { //read from file
      System.out.println("used existing");
      incrementResults = ies.getAllUpdated(mostRecentMarker, true);
    } else {
      System.out.println("used manual");
      incrementResults = ies.getAllUpdated(marker, true);
    }

    mostRecentMarker = incrementResults.getMarker();
    System.out.println("Next marker: \n" + mostRecentMarker);
    Iterable<Map<String, Object>> entities2 = incrementResults.getEntities();
    Iterable<Map<String, Object>> relations2 = incrementResults.getRelations();
    System.out.println("\n Number of entities2: " + Iterables.size(entities2));
    System.out.println("\n Number of relations2: " + Iterables.size(relations2));
  }
}
