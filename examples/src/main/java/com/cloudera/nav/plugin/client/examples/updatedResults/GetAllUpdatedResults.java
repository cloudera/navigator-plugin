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

    UpdatedResults resultsNoMarker = client.getAllUpdated();
    String marker1 = resultsNoMarker.getMarker();
    firstMarker = marker1;
    System.out.println(firstMarker.toString());

    Iterable<Map<String, Object>> entities1 = resultsNoMarker.getEntities();
    System.out.println("Number of entities: " + Iterables.size(entities1));
    Iterable<Map<String, Object>> relations1 = resultsNoMarker.getRelations();
    System.out.println("\n Number of relations: " + Iterables.size(relations1));

    UpdatedResults resultsIncremental = client.getAllUpdated(marker1);
    String marker2 = resultsIncremental.getMarker();
    incrementMarker = marker2;

    Iterable<Map<String, Object>> entities2 = resultsIncremental.getEntities();
    Iterable<Map<String, Object>> relations2 = resultsIncremental.getRelations();
    System.out.println("\n Number of entities2: " + Iterables.size(entities2));
    System.out.println("\n Number of relations2: " + Iterables.size(relations2));

  }
}
