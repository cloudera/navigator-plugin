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

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.examples.lineage.CustomLineageCreator;
import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.MD5IdGenerator;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.Entity;

import org.joda.time.Instant;

/**
 * In this example we show a more complex example of how to create custom entity
 * types and how to link them to hadoop entities. For a description of the base
 * example please see {@link com.cloudera.nav.sdk.examples.lineage.CustomLineageCreator}.
 *
 * As an extension of the previous example, we allow the user to specify custom
 * input and output datasets to a StetsonExecution. We define a new custom
 * entity called StetsonDataset. This is a logical dataset in the Stetson
 * application and is physically represented by an HDFS directory.
 *
 * New relationships:
 *
 * StetsonDataset input ---(LogicalPhysical)---> HDFS directory
 * StetsonDataset output ---(LogicalPhysical)---> HDFS directory
 * StetsonDataset input ---(DataFlow)---> StetsonExecution
 * StetsonExecution ---(DataFlow)---> StetsonDataset output
 */
public class CustomLineageCreator2 extends CustomLineageCreator {

  /**
   * @param args 1. config file path
   *             2. Pig operation id
   *             3. Pig execution id
   *             4. hdfs file system path of input
   *             5. hdfs file system path of output
   */
  public static void main(String[] args) {
    CustomLineageCreator2 lineageCreator = new CustomLineageCreator2(args[0]);
    //lineageCreator.setPigOperationId("bfe554798ad45fd3a169d874768fe2e1");
    //lineageCreator.setPigExecutionId("6b7f61fdce562d2d7825e9eb9dd6d683");
    lineageCreator.setInputPath("/hbase");
    lineageCreator.setOutputPath("/solr");
    lineageCreator.run();
  }

  private String inputPath;
  private String outputPath;

  public CustomLineageCreator2(String configFilePath) {
    super(configFilePath);
  }

  @Override
  protected StetsonExecution2 createExecution(Entity attrs) {
    StetsonExecution2 exec = new StetsonExecution2(plugin.getNamespace());
    exec.setPigExecution(attrs);
    exec.setName("Stetson Execution");
    exec.setDescription("I am an \n F \n B \n I \n agent.");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));

    // Extend the previous stetson example by linking it to inputs and outputs
    String inputName = "StetsonInput"; // Stetson's name for the input dataset
    String outputName = "StetsonOutput"; // Stetson's name for the output data

    NavApiCient client = plugin.getClient();
    Source hdfs = client.getOnlySource(SourceType.HDFS);

    exec.addInput(inputName, getInputPath(), hdfs.getIdentity());
    exec.addOutput(outputName, getOutputPath(), hdfs.getIdentity());

    return exec;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }
}
