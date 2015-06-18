package com.cloudera.nav.plugin.client.examples.updatedResults;

import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.client.UpdatedResults;
import com.google.common.collect.Iterables;

import java.util.Map;

/**
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

    String marker = "{\"4fb48492c18e21ae5be9c2f7faeffe62\":1734,\"4fbdadc6899638782fc8cb626176dc7b\":1732," +
                    "\"efd3b52ca1bebb19990a0a92c7ff6b89\":1733,\"a063e69e6c0660353dc378c836837935\":1732,\"a09b0233cc58ff7d601eaa68673a20c6\":1722}";

    UpdatedResults incrementResults;
    IncrementalExtractionSample ies = new IncrementalExtractionSample(client);
    if (mostRecentMarker != null) {
      System.out.println("used existing");
      incrementResults = ies.getAllUpdated(mostRecentMarker, false);
    } else {
      System.out.println("used manual");
      incrementResults = ies.getAllUpdated(marker, false);
    }

    mostRecentMarker = incrementResults.getMarker();
    System.out.println("Next marker: \n" + mostRecentMarker);
    Iterable<Map<String, Object>> entities2 = incrementResults.getEntities();
    Iterable<Map<String, Object>> relations2 = incrementResults.getRelations();
    System.out.println("\n Number of entities2: " + Iterables.size(entities2));
    System.out.println("\n Number of relations2: " + Iterables.size(relations2));
  }
}
