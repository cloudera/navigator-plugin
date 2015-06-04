package com.cloudera.nav.plugin.client;

import java.util.Iterator;
import java.util.Map;

/**Class for clients to get all updated Entities and Relations and a marker of most recent extractorRunId's for each source,
 * that can then be used in determining future incremental updates.
 *
 * Created by Nadia.Wallace on 6/4/15.
 */
public class UpdatedResults {
    private String marker;
    private Iterable<Map<String, Object>> entities;
    private Iterable<Map<String, Object>> relations;

    public UpdatedResults(String marker, Iterable<Map<String, Object>> entities,
                          Iterable<Map<String, Object>> relations){
        this.marker = marker;
        this.entities = entities;
        this.relations = relations;
    }

    public String getMarker() {
        return marker;
    }

    public Iterable<Map<String, Object>> getEntities() {
        return entities;
    }

    public Iterable<Map<String, Object>> getRelations() {
        return relations;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public void setEntities(Iterable<Map<String, Object>> entities) {
        this.entities = entities;
    }

    public void setRelations(Iterable<Map<String, Object>> relations) {
        this.relations = relations;
    }
}
