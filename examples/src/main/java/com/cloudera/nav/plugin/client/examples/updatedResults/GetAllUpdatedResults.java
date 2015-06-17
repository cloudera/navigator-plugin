package com.cloudera.nav.plugin.client.examples.updatedResults;

import com.cloudera.nav.plugin.client.*;
import com.cloudera.nav.plugin.model.Source;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Map;

/**Example for using getAllUpdated() methods to retrieve entities and relations modified since a given set of extractions.
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
    UpdatedResults resultsNoMarker = ies.getAllUpdated();
    String marker1 = resultsNoMarker.getMarker();
    firstMarker = marker1;
    System.out.println(firstMarker.toString());

    Iterable<Map<String, Object>> entities1 = resultsNoMarker.getEntities();
    System.out.println("Number of entities: " + Iterables.size(entities1));
    Iterable<Map<String, Object>> relations1 = resultsNoMarker.getRelations();
    System.out.println("\n Number of relations: " + Iterables.size(relations1));


  }
}
