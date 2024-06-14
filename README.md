[![Java CI with Maven](https://github.com/holydrug/yandex-backup-util/actions/workflows/maven-build.yml/badge.svg)](https://github.com/holydrug/ytsaurus-query-builder/actions/workflows/ci.yml)
[![Hits-of-Code](https://hitsofcode.com/github/holydrug/ytsaurus-query-builder)](https://hitsofcode.com/github/holydrug/ytsaurus-query-builder/view)

ytsaurus-query-builder
====

ytsaurus-query-builder is an internal DSL and source code generator, modelling the YQL language as a type safe Java API to help you write better YQL. 

## Quick Start
To get the latest release from Maven Central, simply add the following to your build.gradle.kts:

```
implementation("io.github.holydrug:ytsaurus-query-builder:1.0.1")
```

The releases are also available on [Maven Central Repository](https://central.sonatype.com/artifact/io.github.holydrug/ytsaurus-query-builder)!

## Usage

### Create representation scheme of your database table

```kotlin
val document = YtCrudTable(
    YPath.simple("//home/ibox/document"),
    TableSchema.builder()
      .add(ColumnSchema("document_id", string()))
      .add(ColumnSchema("index", int32()))
      .add(ColumnSchema("property_name", string()))
      .add(ColumnSchema("document_date", timestamp()))
      .add(ColumnSchema("status", string()))
      .build(),
    NoYtMapper
  )
```
