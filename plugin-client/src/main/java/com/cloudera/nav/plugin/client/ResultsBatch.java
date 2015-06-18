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

package com.cloudera.nav.plugin.client;

import java.util.Map;

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
