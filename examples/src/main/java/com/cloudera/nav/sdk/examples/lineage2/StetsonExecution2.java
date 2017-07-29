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

package com.cloudera.nav.sdk.examples.lineage2;

import com.cloudera.nav.sdk.examples.lineage.StetsonExecution;
import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.google.common.collect.Lists;

import java.util.Collection;

/**
 * Represents a specific execution of a hypothetical custom application
 * represented by a StetsonScript. Extends the simple stetson example
 * with input/output datasets:
 *
 * 1. We associate StetsonExecution instances with input and output
 *    StetsonDatasets.
 * 2. Each StetsonDataset is a logical dataset that Stetson users see and
 *    is backed by an HDFS directory
 * 3. When metadata is writte, we form data-flow relationships between the
 *    StetsonDataset's and the StetsonExecution. We also form logical-physical
 *    relationships between the StetsonDataset and the HDFS directory (as
 *    specified by StetsonDataset)
 */
@MClass(model = "stetson_exec")
public class StetsonExecution2 extends
    StetsonExecution {

  @MRelation(role=RelationRole.SOURCE)
  private Collection<StetsonDataset> inputs;
  @MRelation(role=RelationRole.TARGET)
  private Collection<StetsonDataset> outputs;

  /**
   * @param namespace
   */
  public StetsonExecution2(String namespace) {
    super(namespace);
  }

  /**
   * @return all input datasets
   */
  public Collection<StetsonDataset> getInputs() {
    return inputs;
  }

  /**
   * @return all output datasets
   */
  public Collection<StetsonDataset> getOutputs() {
    return outputs;
  }

  /**
   * Reset all input datasets to the given collection of StetsonDataset's
   * @param inputs
   */
  public void setInputs(Collection<StetsonDataset> inputs) {
    this.inputs = Lists.newArrayList(inputs);
  }

  /**
   * Reset all output datasets to the given collection of StetsonDataset's
   * @param outputs
   */
  public void setOutputs(Collection<StetsonDataset> outputs) {
    this.outputs = Lists.newArrayList(outputs);
  }

  /**
   * Add a new input dataset backed by the given HDFS directory
   * Not threadsafe
   * @param name
   * @param path
   * @param sourceId
   */
  public void addInput(String name, String path, String sourceId) {
    if (inputs == null) {
      inputs = Lists.newArrayList();
    }
    inputs.add(new StetsonDataset(name, getNamespace(), path, sourceId));
  }

  /**
   * Add a new output dataset backed by the given HDFS directory
   * Not threadsafe
   * @param name
   * @param path
   */
  public void addOutput(String name, String path, String sourceId) {
    if (outputs == null) {
      outputs = Lists.newArrayList();
    }
    outputs.add(new StetsonDataset(name, getNamespace(), path, sourceId));
  }
}
