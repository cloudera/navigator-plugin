#!/bin/bash

mvn -f ../examples/pom.xml -X exec:java -Dexec.mainClass="com.cloudera.nav.sdk.examples.extraction.UpdateMetadata" -Dexec.args="$1 $2"
