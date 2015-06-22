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

package com.cloudera.nav.plugin.model;

import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collection;

import org.junit.Test;

public class ValidationUtilTest {

  private static class MetadataClass {

    @MProperty(required=true)
    public Collection<String> getColl() {
      return ImmutableList.of("foo", "bar");
    }

    @MProperty(required=true)
    public String getStr() {
      return "foo";
    }

    @MProperty(required=true)
    public int getInt() {
      return 5;
    }
  }

  private static class BadColl extends MetadataClass {
    @MProperty(required = true)
    public Collection<String> getEmptyColl() {
      return Lists.newArrayList();
    }
  }

  private static class BadString extends MetadataClass {
    @MProperty(required = true)
    public String getEmpty() {
      return "";
    }
  }

  private static class BadObj extends MetadataClass {
    @MProperty(required = true)
    public Long getNull() {
      return null;
    }
  }

  @Test
  public void testBasic() {
    MetadataClass mclassObj = new MetadataClass();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadColl() {
    BadColl mclassObj = new BadColl();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadStr() {
    BadString mclassObj = new BadString();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadObj() {
    BadObj mclassObj = new BadObj();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

}
