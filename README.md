Navigator SDK Java Client
=========================

[![Build Status](https://travis-ci.org/cloudera/navigator-sdk.svg?branch=master)](https://travis-ci.org/cloudera/navigator-sdk)

The Cloudera Navigator SDK is a client library that provides functionality to
help users extract metadata from Navigator and to enrich the metadata in
Navigator with custom metadata models, entities, and relationships.

Sample Use Cases
----------------

*Incremental Metadata Extraction*

Certain applications need to extract metadata from Navigator for their own
 purposes or to integrate into an enterprise-wide metadata management system.
 For almost any production Hadoop cluster, it is not feasible to do a full
  extraction of all available metadata every time. Instead, the Navigator SDK
  provides [code examples](examples/src/main/java/com/cloudera/nav/sdk/examples/extraction/IncrementalExtraction.java)
  to extract metadata in an incremental fashion.

*Custom Metadata Entities and Lineage Augmentation*

Many ETL and analytics applications built on top of Hadoop use a custom DSL to
work with data. For their enterprise customers, Navigator can serve as the
defacto governance solution. The Navigator SDK provides critical functionality
that allows partner applications to create custom metadata models in Navigator
and link the custom concepts in the external application to the underlying
files, directories, and operations in Hadoop.

As an example, let’s suppose there is a company Foo which makes a data
preparation application. Foo allows its users to create a data preparation
pipeline using a custom DSL. The custom DSL script is then implemented as Pig
jobs. The Navigator SDK allows Foo to create custom entities in Navigator
to represent the custom DSL operations, executions, and input/output datasets.
It also allows Foo to then create relationships linking those custom
entities to the underlying Pig jobs and executions. This makes it much easier
for the end user to search and analyze provenance in terms of the custom DSL.

See code examples [here](examples/src/main/java/com/cloudera/nav/sdk/examples/lineage)
and [here](examples/src/main/java/com/cloudera/nav/sdk/examples/lineage2)
 for details.

*Custom Dataset Schema Definition*

Data profiling applications often infer schema from CSV, JSON, and other files
in Hadoop. In order to help provide a data governance layer for those
 applications, the Navigator SDK can be used to [augment](examples/src/main/java/com/cloudera/nav/sdk/examples/schema)
 the file metadata with the inferred schemas. The schema information can then
 be used to integrate with Navigator's policy engine to perform tasks such as
  tagging columns as private information.

*Setting Custom Metadata*

The Navigator API allows users to set tags and custom key-value pairs on
metadata entities. The Navigator SDK provides an easy-to-use interface for users
 to take advantage of those APIs. The [code examples](examples/src/main/java/com/cloudera/nav/sdk/examples/tags)
 included in the SDK demonstrates setting tags for HDFS and Hive entities.


Navigator Metadata
------------------

The metadata managed by Navigator centers around three main concepts -
[entity](model/src/main/java/com/cloudera/nav/sdk/model/entities/Entity.java),
[source](model/src/main/java/com/cloudera/nav/sdk/model/Source.java),
and [relation](model/src/main/java/com/cloudera/nav/sdk/model/relations/Relation.java).
Entities are objects defined by Hadoop components and
services like HDFS, Hive, MapReduce, Pig, etc. Examples of entities include
HDFS files and directories, Hive databases, tables, columns, queries, MR jobs,
job instances, and more. For each Hadoop object, Navigator defines a
corresponding entity subtype.

Each entity has a source which is the Hadoop service or component that created
the entity. Each source has a source type which is the type of service
represented. For example, Navigator may manage multiple HDFS services with names
 from HDFS-1, HDFS-2, and so on. All of them will have the same source type, which
  is HDFS.

Entities are also associated with an entity type, which is the logical type of
the object being represented by the entity. This could be a file, directory,
database, table, column, etc.

The third major concept is that of a relation. A relation defines the
connections between different entities. A relation is defined by two
endpoints and is adirectional by itself. Each endpoint must have one or more
entities that have the same entity type and source. A relation is also
associated with a relation type. Navigator defines several relation types and
for each one, Navigator defines a relation subtype to make the construction of
these relationships easier. The relation types are:

- [DataFlow](model/src/main/java/com/cloudera/nav/sdk/model/relations/DataFlowRelation.java) -
the endpoints are sources and targets indicating movement of data
(e.g., query input -> query output)

- [ParentChild](model/src/main/java/com/cloudera/nav/sdk/model/relations/ParentChildRelation.java) -
the endpoints are parent and child(ren) indicating containment
(e.g., directory -> files)

- [LogicalPhysical](model/src/main/java/com/cloudera/nav/sdk/model/relations/LogicalPhysicalRelation.java) -
the endpoints are logical and physical indicating the
connection between an abstract entity and its concrete manifestation (e.g.,
Hive table -> HDFS directory which has the data)

- [InstanceOf](model/src/main/java/com/cloudera/nav/sdk/model/relations/InstanceOfRelation.java) -
the endpoints are template and instance indicating the connection
between an operation and its execution.



Custom Entities and Relations
-----------------------------

In order to create custom entities, the Navigator client library provides enums,
annotations, and a plugin to help partner applications define and register custom
entities. A sample custom entity might look like the following:

```java
@MClass(model = "stetson_op")
public class StetsonScript extends Entity {

  @MRelation(role = RelationRole.PHYSICAL)
  private EndPointProxy pigOperation;
  @MProperty
  private String script;

  public String getScript() {
    return script;
  }

  @Override
  public EntityType getType() {
    return EntityTypes.OPERATION;
  }

  public Entity getPigOperation() {
    return pigOperation;
  }
}
```

The client library provides enums like
[EntityType](model/src/main/java/com/cloudera/nav/sdk/model/entities/EntityType.java),
[RelationType](model/src/main/java/com/cloudera/nav/sdk/model/relations/RelationType.java),
and [RelationRole](model/src/main/java/com/cloudera/nav/sdk/model/relations/RelationRole.java) used in the above example.
The [@MClass](model/src/main/java/com/cloudera/nav/sdk/model/annotations/MClass.java)
annotation indicates that the StetsonScript class is a custom metadata model.
The [@MProperty](model/src/main/java/com/cloudera/nav/sdk/model/annotations/MProperty.java)
annotation indicates that script is a model field. The
[@MRelation](model/src/main/java/com/cloudera/nav/sdk/model/annotations/MRelation.java)
annotation in the example indicates that StetsonScript entity should be
connected to the given Pig operation entity with a logical-physical relation
(where the Pig operation is the physical entity).



Writing to Navigator
--------------------

Once the custom metadata models have been defined, users can use the
[NavigatorPlugin](client/src/main/java/com/cloudera/nav/sdk/client/NavigatorPlugin.java)
class to write custom metadata to Navigator. Creating a NavigatorPlugin requires
a set of configurations that includes properties like the Navigator API URL. See
the provided [sample configuration](examples/src/main/resources/sample.conf)
for details.

An example workflow might look like the following:

```java
// Create the plugin
NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(
  "/home/chang/.navigator/navigator_sdk.conf");

// Create the template
StetsonScript script = new StetsonScript();
PigOperation pigOperation = new PigOperation("logicalPlanHash","JobName");
script.setNamespace(config.getNamespace());
script.setIdentity(script.generateId());
script.setName("myScript");
script.setPigOperation(pigOperation);

// Create the instance
StetsonExecution exec = new StetsonExecution();
PigOperationExecution pigExecution = new PigOperationExecution("ScriptId","JobName");
exec.setNamespace(config.getNamespace());
exec.setName("myExecution");
exec.setTemplate(script);
exec.setPigExecution(pigExecution);

// Write the metadata
plugin.write(exec);
```

- With this new version of the SDK which supports Cloudera Navigator 2.9 and above,
  in order to add tags, custom metadata or managed metadata to Hadoop Entities
  or to link to Hadoop entities, users would have to either provide the ID of the Hadoop
  Entity(which is generated by Navigator) or the attributes required to generate the ID.
  See example [code examples](examples/src/main/java/com/cloudera/nav/sdk/examples/lineage/CustomLineageCreator.java)

Creating Managed Metadata Models
--------------------------------

Cloudera Navigator 2.6.0 added the ability to create manage metadata fields.
Managed metadata is a more organized way to extend metadata properties collected
by Navigator. Managed metadata properties must be declared with a type
constraint on the value(s), must belong to a namespace, and can be associated
with 0 or more metadata classes.

The SDK client is configured with a namespace that's used for all declared
managed metadata properties. It is also the package name for all custom metadata
classes. Just as a reminder, a custom metadata class can be created by
subclassing Entity with an @MClass annotation.

New metadata properties for custom metadata classes are created using the
@MProperty annotation. In this new version of the SDK, we've added new
attributes to the annotation to make it easier to register managed metadata
properties. In order to create a managed property, set the `register` attribute
to `true`. You can then set `fieldType` (default `TEXT`), `pattern` for regex
matching, `maxLength` for `TEXT` fields, or an array of `values` for `ENUM`
properties. The examples have been updated to demonstrate this new capability.



Usage Notes
-----------

- If you have configuration variables already setup elsewhere, instead of using
`NavigatorPlugin.fromConfigFile` you can use `NavigatorPlugin.fromConfigMap` and
 pass in a `Map` of configurations.

- By default, Navigator throttles its own internal committing process so that
  new metadata won't be available immediately. For testing and development,
  you can add a configuration variable "autocommit=true". However, it is very
  strongly recommended that you do not use this in production in order to avoid
  performance degradation and memory usage issues when running at scale.