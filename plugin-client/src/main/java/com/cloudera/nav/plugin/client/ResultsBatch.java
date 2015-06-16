package com.cloudera.nav.plugin.client;

import com.cloudera.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;

/**
 * Created by Nadia.Wallace on 6/11/15.
 */
public class ResultsBatch  {
  private String cursorMark;
  private Map<String, Object>[] results;

  public Map<String, Object>[] getResults() { return results;  }

  public void setResults(Map<String, Object>[] results)  { this.results = results; }

  public String getCursorMark(){ return cursorMark;  }

  public void setCursorMark(String cursorMark) { this.cursorMark = cursorMark; }

}
