---
title: Overview
weight: 1
type: docs
aliases:
  - /dev/table/
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Table API & SQL

Apache Flink features two relational APIs - the Table API and SQL - for unified stream and batch
processing. The Table API is a language-integrated query API for Java, Scala, and Python that
allows the composition of queries from relational operators such as selection, filter, and join in
a very intuitive way. Flink's SQL support is based on [Apache Calcite](https://calcite.apache.org)
which implements the SQL standard. Queries specified in either interface have the same semantics
and specify the same result regardless of whether the input is continuous (streaming) or bounded (batch).

The Table API and SQL interfaces integrate seamlessly with each other and Flink's DataStream API. 
You can easily switch between all APIs and libraries which build upon them.
For instance, you can detect patterns from a table using [`MATCH_RECOGNIZE` clause]({{< ref "docs/dev/table/sql/queries/match_recognize" >}})
and later use the DataStream API to build alerting based on the matched patterns.

## Getting started

Depending on the target programming language, you need to add the Java or Scala API to a project
in order to use the Table API & SQL for defining pipelines.

{{< tabs "94f8aceb-507f-4c8f-977e-df00fe903203" >}}
{{< tab "Java" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Scala" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
</dependency>
```
{{< /tab >}}
{{< tab "Python" >}}
{{< stable >}}
```bash
$ python -m pip install apache-flink {{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ python -m pip install apache-flink
```
{{< /unstable >}}
{{< /tab >}}
{{< /tabs >}}

If you need to use Table API together with DataStream API, use the following package instead:

{{< tabs "94f8aceb-507f-4c8f-977e-df00fe903203" >}}
{{< tab "Java" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Scala" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
</dependency>
```
{{< /tab >}}
{{< tab "Python" >}}
{{< stable >}}
```bash
$ python -m pip install apache-flink {{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ python -m pip install apache-flink
```
{{< /unstable >}}
{{< /tab >}}
{{< /tabs >}}

For more details on a complete configuration of the project, 
including how to test your pipelines locally and how to create an uber Job JAR with your dependencies, 
check out [Project configuration]({{< ref "docs/dev/table/project-configuration" >}}). <!-- This page doesn't exist, create it -->

### Extension Dependencies

If you want to implement a set of [user-defined functions]({{< ref "docs/dev/table/functions/udfs" >}}),
the following dependency is sufficient and can be used for JAR files for the SQL Client:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```

For implementation of custom formats or connectors, refer to [User-defined Sources & Sinks]({{< ref "docs/dev/table/sourcesSinks" >}}).

{{< top >}}

### Testing

If you want to test the Table API & SQL programs locally within your IDE, you can add the following dependency:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-test-utils</artifactId>
    <version>{{< version >}}</version>
    <scope>test</scope>
</dependency>
```

This will automatically bring in the query planner and the runtime, required respectively to plan and execute the queries.

<!-- Advanced topics -->

### Distribution

The Flink distribution contains by default the required JARs to execute Flink SQL Jobs in `/lib`, in particular:

* `flink-table-api-java-uber-{{< version >}}.jar` containing all the Java APIs
* `flink-table-runtime-{{< version >}}.jar` containing the runtime
* `flink-table-planner-loader-{{< version >}}.jar` containing the query planner

For Scala APIs, formats and connectors you need to either download and manually include the JARs in the `/lib` folder (recommended), or you need to shade them in the uber JAR of your Flink SQL Jobs.
For more details, check out [Connect to External Systems]({{< ref "docs/connectors/table/overview" >}}).

#### Planner and Planner loader

Starting from Flink 1.15, the distribution contains two planners:

* `flink-table-planner{{< scala_version >}}-{{< version >}}.jar`, in `/opt`, contains the query planner.
* `flink-table-planner-loader-{{< version >}}.jar`, loaded by default in `/lib`, contains the query planner hidden behind an isolated classpath. That is, you won't be able to address directly any `io.apache.flink.table.planner`.

If you need to access and use the internals of the query planner, you can swap the two JARs, but be aware that you will be constrained to use the Scala version of the Flink distribution you're using.

Note: the two planners cannot co-exist at the same time in the classpath, that is if you load both of them in `/lib` your Table Jobs will fail.

Where to go next?
-----------------

* [Concepts & Common API]({{< ref "docs/dev/table/common" >}}): Shared concepts and APIs of the Table API and SQL.
* [Data Types]({{< ref "docs/dev/table/types" >}}): Lists pre-defined data types and their properties.
* [Streaming Concepts]({{< ref "docs/dev/table/concepts/overview" >}}): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Connect to External Systems]({{< ref "docs/connectors/table/overview" >}}): Available connectors and formats for reading and writing data to external systems.
* [Table API]({{< ref "docs/dev/table/tableApi" >}}): Supported operations and API for the Table API.
* [SQL]({{< ref "docs/dev/table/sql/overview" >}}): Supported operations and syntax for SQL.
* [Built-in Functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}): Supported functions in Table API and SQL.
* [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}): Play around with Flink SQL and submit a table program to a cluster without programming knowledge.

{{< top >}}
