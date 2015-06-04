package com.cloudera.nav.plugin.client.examples.updatedResults;

import com.cloudera.nav.plugin.client.*;

import java.util.Map;

/**Example for using getAllUpdated() methods to retrieve entities and relations modified since a given set of extractions.
 *
 * Created by Nadia.Wallace on 6/4/15.
 */
public class GetAllUpdatedResults {

    public static void main(String[] args){
        // setup the plugin and api client
        String configFilePath = args[0];
        PluginConfigurations config = (new PluginConfigurationFactory())
                .readConfigurations(configFilePath);
        NavigatorPlugin plugin = new NavigatorPlugin(config);
        NavApiCient client = new NavApiCient(config);

        UpdatedResults resultsNoMarker = client.getAllUpdated();
        String marker1 = resultsNoMarker.getMarker();
        Iterable<Map<String, Object>> entities1 = resultsNoMarker.getEntities();
        Iterable<Map<String, Object>> relations1 = resultsNoMarker.getRelations();

        UpdatedResults resultsIncremental = client.getAllUpdated(marker1);
        String marker2 = resultsNoMarker.getMarker();
        Iterable<Map<String, Object>> entities2 = resultsNoMarker.getEntities();
        Iterable<Map<String, Object>> relations2 = resultsNoMarker.getRelations();
    }
}
