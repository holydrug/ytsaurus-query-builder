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

### YQL select without join with all criterias

```kotlin
val ytQl = YTQLBuilder.from(document)
      .selectAll()
      .where("document_id", did)
      .whereNot("index", 3)
      .whereGreater("index", 11)
      .whereLess("index", 15)
      .whereGreater("document_date", date, timestamp())
      .whereLess("document_date", date, timestamp())
      .whereNot("property_name", "not")
      .whereIn("property_name", listOf("pg", "pid", "rid"))
      .whereIn("status", listOf(1, 2, 3))
      .orderBy("document_id" to OrderDirection.ASC)
      .limit(10)
      .build()
      .query
```

### YQL practical select

```kotlin
suspend fun findAllFiltered(
    filter: ReceiptFilter,
    currentUserTin: String? = null
  ): List<ReceiptEntity> = YTQLBuilder.from(schema.receipt)
    .selectAll()
    .apply {
      filter.sellerTin?.let { where("seller_tin", it) }
      filter.status?.takeIf { it.isNotEmpty() }?.let { whereIn("status", it, string()) }
      filter.operationType?.takeIf { it.isNotEmpty() }?.let { whereIn("operation_type", it, string()) }
      filter.cursor?.let { whereLess("id", it, string()) }
      filter.receiptDateFrom?.let { whereGreater("receipt_date", it, datetime()) }
      filter.receiptDateTo?.let { whereLess("receipt_date", it, datetime()) }
      filter.createdOnFrom?.let { whereGreater("created_on", it, timestamp()) }
      filter.createdOnTo?.let { whereLess("created_on", it, timestamp()) }
      filter.ids?.let { uuids -> whereIn("id", uuids, string()) }
      filter.originalIds?.let { whereIn("original_id", it) }
      currentUserTin?.let { whereOr("seller_tin" to it, "buyer_tin" to it) }
    }.orderBy("created_on" to OrderDirection.DESC)
    .limit(filter.limit)
    .build()
    .let { select ->
      client.tablet { tx ->
        tx.selectRows(
          select
        ).asyncAwait()
      }.yTreeRows
    }.map { schema.receipt.mapper.fromYt(it) }
```
