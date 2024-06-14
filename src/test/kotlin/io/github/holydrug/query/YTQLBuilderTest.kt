package io.github.holydrug.query

import io.github.holydrug.mapper.extension.toEpochMicro
import io.github.holydrug.mapper.impl.NoYtMapper
import io.github.holydrug.query.builder.YTQLBuilder
import io.github.holydrug.query.builder.YTQLJoinBuilder
import io.github.holydrug.query.model.order.OrderDirection
import io.github.holydrug.query.model.selector.YtSelector.Companion.avg
import io.github.holydrug.query.model.selector.YtSelector.Companion.column
import io.github.holydrug.query.model.selector.YtSelector.Companion.count
import io.github.holydrug.query.model.selector.YtSelector.Companion.max
import io.github.holydrug.query.model.selector.YtSelector.Companion.min
import io.github.holydrug.query.model.selector.YtSelector.Companion.sum
import io.github.holydrug.scheme.YtCrudTable
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.ColumnSchema
import tech.ytsaurus.core.tables.ColumnSortOrder
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.typeinfo.TiType.bool
import tech.ytsaurus.typeinfo.TiType.datetime
import tech.ytsaurus.typeinfo.TiType.doubleType
import tech.ytsaurus.typeinfo.TiType.floatType
import tech.ytsaurus.typeinfo.TiType.int32
import tech.ytsaurus.typeinfo.TiType.optional
import tech.ytsaurus.typeinfo.TiType.string
import tech.ytsaurus.typeinfo.TiType.timestamp
import tech.ytsaurus.typeinfo.TiType.uint64
import tech.ytsaurus.typeinfo.TiType.yson
import java.time.Instant
import java.util.UUID

class YTQLBuilderTest {

  private val document = YtCrudTable(
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

  private val receiptItem = YtCrudTable(
    YPath.simple("//home/ibox-ng/rcp/receipt_item"),
    TableSchema.builder().setUniqueKeys(true).setStrict(true)
      .add(ColumnSchema("receipt_id", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("id", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("name", string()))
      .add(ColumnSchema("price", doubleType()))
      .add(ColumnSchema("vat_rate", floatType()))
      .add(ColumnSchema("quantity", doubleType()))
      .build(),
    NoYtMapper
  )

  private val receiptItemCode = YtCrudTable(
    YPath.simple("//home/ibox-ng/rcp/receipt_item_code"),
    TableSchema.builder().setUniqueKeys(true).setStrict(true)
      .add(ColumnSchema("receipt_id", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("receipt_item_id", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("id", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("code", string()))
      .add(ColumnSchema("product_id", optional(string())))
      .add(ColumnSchema("partial_quantity", optional(floatType())))
      .add(ColumnSchema("processing_error_code", optional(string())))
      .add(ColumnSchema("processing_error_parameters", optional(yson())))
      .build(),
    NoYtMapper
  )

  private val receipt = YtCrudTable(
    YPath.simple("//home/ibox-ng/rcp/receipt"),
    TableSchema.builder().setUniqueKeys(true).setStrict(true)
      .add(ColumnSchema("id", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("original_id", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("raw_receipt_id", string()))
      .add(ColumnSchema("cash_id", string()))
      .add(ColumnSchema("source", string()))
      .add(ColumnSchema("seller_id", optional(uint64())))
      .add(ColumnSchema("seller_tin", string()))
      .add(ColumnSchema("sales_address", optional(string())))
      .add(ColumnSchema("buyer_id", optional(uint64())))
      .add(ColumnSchema("buyer_tin", optional(string())))
      .add(ColumnSchema("receipt_type", string()))
      .add(ColumnSchema("created_on", timestamp()))
      .add(ColumnSchema("receipt_date", datetime()))
      .add(ColumnSchema("operation_type", string()))
      .add(ColumnSchema("status", string()))
      .add(ColumnSchema("processing_error_code", optional(string())))
      .add(ColumnSchema("processing_error_parameters", optional(yson())))
      .build(),
    NoYtMapper
  )

  private val code = YtCrudTable(
    YPath.simple("//home/ibox/code/code"),
    TableSchema.builder()
      .setUniqueKeys(true)
      .setStrict(true)
      .add(ColumnSchema("code", string(), ColumnSortOrder.ASCENDING))
      .add(ColumnSchema("template", optional(string())))
      .add(ColumnSchema("emission_type", optional(string())))
      .add(ColumnSchema("original_release_method", optional(string())))
      .add(ColumnSchema("connected_code", optional(string())))
      .add(ColumnSchema("emission_date", timestamp()))
      .add(ColumnSchema("issue_date", timestamp()))
      .add(ColumnSchema("validation_date", optional(timestamp())))
      .add(ColumnSchema("utilisation_date", optional(timestamp())))
      .add(ColumnSchema("introduction_date", optional(timestamp())))
      .add(ColumnSchema("last_event_business_datetime", datetime()))
      .add(ColumnSchema("product_id", optional(string())))
      .add(ColumnSchema("product_group", int32()))
      .add(ColumnSchema("category", optional(string())))
      .add(ColumnSchema("package_type", string()))
      .add(ColumnSchema("specific_characteristic", optional(string())))
      .add(ColumnSchema("issuer_party_kind_id", int32()))
      .add(ColumnSchema("owner_tin", string()))
      .add(ColumnSchema("seller_tin", optional(string())))
      .add(ColumnSchema("owner_party_kind_id", optional(int32())))
      .add(ColumnSchema("contractor_party_kind_id", optional(int32())))
      .add(ColumnSchema("owner_business_place_id", optional(int32())))
      .add(ColumnSchema("issuer_business_place_id", optional(int32())))
      .add(ColumnSchema("contractor_business_place_id", optional(int32())))
      .add(ColumnSchema("status", string()))
      .add(ColumnSchema("extended_status", optional(string())))
      .add(ColumnSchema("parent_code", optional(string())))
      .add(ColumnSchema("actual_package_composition", optional(yson())))
      .add(ColumnSchema("original_package_composition", optional(yson())))
      .add(ColumnSchema("aggregation_date", optional(timestamp())))
      .add(ColumnSchema("former_parent_code", optional(string())))
      .add(ColumnSchema("parent_change_datetime", optional(datetime())))
      .add(ColumnSchema("packing_change_datetime", optional(datetime())))
      .add(ColumnSchema("empty_package", optional(bool())))
      .add(ColumnSchema("withdrawal_reason", optional(string())))
      .add(ColumnSchema("withdrawal_datetime", optional(datetime())))
      .add(ColumnSchema("return_reason", optional(string())))
      .add(ColumnSchema("return_datetime", optional(datetime())))
      .add(ColumnSchema("partial_quantity", optional(doubleType())))
      .add(ColumnSchema("sale_datetime", optional(datetime())))
      .add(ColumnSchema("return_receipt_datetime", optional(datetime())))
      .add(ColumnSchema("payment_timestamp", optional(timestamp())))
      .add(ColumnSchema("payment_type", optional(string())))
      .add(ColumnSchema("tariff_id", optional(int32())))
      .add(ColumnSchema("charge_id", optional(string())))
      .add(ColumnSchema("production_date", optional(datetime())))
      .add(ColumnSchema("product_series", optional(string())))
      .add(ColumnSchema("expiration_date", optional(datetime())))
      .add(ColumnSchema("customs_declaration", optional(string())))
      .build(),
    NoYtMapper
  )

  private val receiptIds = setOf(
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
  )

  @Test
  fun selectWithAllCriteria() {
    val did = UUID.randomUUID().toString()
    val date = Instant.now()
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

    assertEquals(
      "* FROM [${document.name}] WHERE document_id = \"$did\" AND index != 3 AND index > 11 AND index < 15 AND document_date > ${date.toEpochMicro()} AND document_date < ${date.toEpochMicro()} AND property_name != \"not\" AND property_name IN (\"pg\", \"pid\", \"rid\") AND status IN (1, 2, 3) ORDER BY document_id ASC LIMIT 10",
      ytQl
    )
  }

  @Test
  fun selectWithAllCriteriaWithSpecific() {
    val did = UUID.randomUUID().toString()
    val date = Instant.now()
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

    assertEquals(
      "* FROM [${document.name}] WHERE document_id = \"$did\" AND index != 3 AND index > 11 AND index < 15 AND document_date > ${date.toEpochMicro()} AND document_date < ${date.toEpochMicro()} AND property_name != \"not\" AND property_name IN (\"pg\", \"pid\", \"rid\") AND status IN (1, 2, 3) ORDER BY document_id ASC LIMIT 10",
      ytQl
    )
  }

  @Test
  fun selectExactColumns() {
    val ytQl = YTQLBuilder.from(document)
      .select("document_id", "property_name", "index")
      .build()
      .query

    assertEquals(
      "document_id, property_name, index FROM [${document.name}]",
      ytQl
    )
  }

  @Test
  fun selectExactSpecificColumns() {
    val ytQl = YTQLJoinBuilder.from(document)
      .select(document["document_id", "property_name", "index"])
      .build()
      .query

    assertEquals(
      "document.document_id, document.property_name, document.index FROM [${document.name}] AS document",
      ytQl
    )
  }

  @Test
  fun selectCountWithHaving() {
    val ytQl = YTQLBuilder.from(receipt)
      .select(
        count("status_count"),
        column("status")
      ).groupBy("status")
      .havingIn("status", listOf("CREATED", "IN_PROCESSING"))
      .build()
      .query

    assertEquals(
      "SUM(1) AS status_count, status FROM [//home/ibox-ng/rcp/receipt] GROUP BY status HAVING status IN (\"CREATED\", \"IN_PROCESSING\")",
      ytQl
    )
  }

  @Test
  fun selectSum() {
    val ytQl = YTQLBuilder.from(code)
      .select(
        sum("expiration_date" to "expiration_date_sum"),
        column("package_type", "expiration_date")
      ).groupBy("package_type", "expiration_date")
      .build()
      .query

    assertEquals(
      "SUM(expiration_date) AS expiration_date_sum, package_type, expiration_date FROM [//home/ibox/code/code] GROUP BY package_type, expiration_date",
      ytQl
    )
  }

  @Test
  fun selectMax() {
    val ytQl = YTQLBuilder.from(code)
      .select(
        max(
          "expiration_date" to "expiration_date_max",
          "emission_date" to "emission_date_max"
        ),
        column("package_type", "expiration_date", "emission_date")
      ).groupBy("package_type", "expiration_date", "emission_date")
      .build()
      .query

    assertEquals(
      "MAX(expiration_date) AS expiration_date_max, MAX(emission_date) AS emission_date_max, package_type, expiration_date, emission_date FROM [//home/ibox/code/code] GROUP BY package_type, expiration_date, emission_date",
      ytQl
    )
  }

  @Test
  fun selectMin() {
    val ytQl = YTQLBuilder.from(code)
      .select(
        min(
          "expiration_date" to "expiration_date_min",
          "emission_date" to "emission_date_min"
        ),
        column("package_type", "expiration_date", "emission_date")
      ).groupBy("package_type", "expiration_date", "emission_date")
      .build()
      .query

    assertEquals(
      "MIN(expiration_date) AS expiration_date_min, MIN(emission_date) AS emission_date_min, package_type, expiration_date, emission_date FROM [//home/ibox/code/code] GROUP BY package_type, expiration_date, emission_date",
      ytQl
    )
  }

  @Test
  fun selectAvg() {
    val ytQl = YTQLBuilder.from(code)
      .select(
        avg("product_group" to "product_group_avg"),
        column("package_type", "product_group")
      ).groupBy("package_type", "product_group")
      .build()
      .query

    assertEquals(
      "AVG(product_group) AS product_group_avg, package_type, product_group FROM [//home/ibox/code/code] GROUP BY package_type, product_group",
      ytQl
    )
  }

  @Test
  fun selectCountWithHavingAndOrderBy() {
    val ytQl = YTQLBuilder.from(receipt)
      .select(
        count("status_count"),
        column("status", "created_on")
      ).groupBy("status", "created_on")
      .havingIn("status", listOf("CREATED", "IN_PROCESSING"))
      .orderBy("created_on" to OrderDirection.DESC)
      .limit(100)
      .build()
      .query

    assertEquals(
      "SUM(1) AS status_count, status, created_on FROM [//home/ibox-ng/rcp/receipt] GROUP BY status, created_on HAVING status IN (\"CREATED\", \"IN_PROCESSING\") ORDER BY created_on DESC LIMIT 100",
      ytQl
    )
  }

  @Test
  fun selectCountWithWhere2() {
    val ytQl = YTQLBuilder.from(receipt)
      .select(
        count("status_count"),
        column("status")
      ).groupBy("status")
      .whereIn("status", listOf("CREATED", "IN_PROCESSING"))
      .build()
      .query

    assertEquals(
      "SUM(1) AS status_count, status FROM [//home/ibox-ng/rcp/receipt] WHERE status IN (\"CREATED\", \"IN_PROCESSING\") GROUP BY status",
      ytQl
    )
  }

  @Test
  fun selectCountWithWhere3() {
    val ytQl = YTQLBuilder.from(receipt)
      .select(
        count("status_count"),
        column("status")
      ).whereIn("status", listOf("CREATED", "IN_PROCESSING"))
      .groupBy("status")
      .build()
      .query

    assertEquals(
      "SUM(1) AS status_count, status FROM [//home/ibox-ng/rcp/receipt] WHERE status IN (\"CREATED\", \"IN_PROCESSING\") GROUP BY status",
      ytQl
    )
  }

  @Test
  fun selectCountWithHaving3() {
    val ytQl = YTQLBuilder.from(receipt)
      .select(
        count("total_count"),
        column("status", "cash_id")
      ).groupBy("status", "cash_id")
      .havingIn("status", listOf("CREATED", "IN_PROCESSING"))
      .build()
      .query

    assertEquals(
      "SUM(1) AS total_count, status, cash_id FROM [//home/ibox-ng/rcp/receipt] GROUP BY status, cash_id HAVING status IN (\"CREATED\", \"IN_PROCESSING\")",
      ytQl
    )
  }

  @Test
  fun selectCountWithHaving2() {
    val ytQl = YTQLBuilder.from(receipt)
      .select(
        count("total_count"),
        column("status", "cash_id")
      ).groupBy("status", "cash_id")
      .havingIn("status", listOf("CREATED", "IN_PROCESSING"))
      .build()
      .query

    assertEquals(
      "SUM(1) AS total_count, status, cash_id FROM [//home/ibox-ng/rcp/receipt] GROUP BY status, cash_id HAVING status IN (\"CREATED\", \"IN_PROCESSING\")",
      ytQl
    )
  }

  @Test
  fun selectCountWithHaving4() {
    val ytQl = YTQLBuilder.from(receipt)
      .select(
        count("total_count"),
        column("status", "cash_id")
      ).whereGreater("created_on", 1717778719746657)
      .where("receipt_type", "SALE")
      .groupBy("status", "cash_id")
      .havingIn("status", listOf("CREATED", "IN_PROCESSING"))
      .havingNot("cash_id", "string")
      .build()
      .query

    assertEquals(
      "SUM(1) AS total_count, status, cash_id FROM [//home/ibox-ng/rcp/receipt] WHERE created_on > 1717778719746657 AND receipt_type = \"SALE\" GROUP BY status, cash_id HAVING cash_id != \"string\" AND status IN (\"CREATED\", \"IN_PROCESSING\")",
      ytQl
    )
  }

  @Test
  fun selectOr() {
    val ytQl = YTQLBuilder.from(document)
      .selectAll()
      .whereOr("seller_tin" to "123", "buyer_tin" to "123")
      .where("seller_tin", "string")
      .orderBy("created_on" to OrderDirection.DESC)
      .limit(10)
      .build()
      .query

    assertEquals(
      "* FROM [${document.name}] WHERE seller_tin = \"string\" AND (seller_tin = \"123\" OR buyer_tin = \"123\") ORDER BY created_on DESC LIMIT 10",
      ytQl
    )
  }

  @Test
  fun testOrderBy() {
    val ytQl = YTQLBuilder.from(document)
      .selectAll()
      .whereOr("seller_tin" to "123", "buyer_tin" to "123")
      .where("seller_tin", "string")
      .orderBy(
        "created_on" to OrderDirection.DESC,
        "seller_tin" to OrderDirection.ASC
      ).limit(10)
      .build()
      .query

    assertEquals(
      "* FROM [${document.name}] WHERE seller_tin = \"string\" AND (seller_tin = \"123\" OR buyer_tin = \"123\") ORDER BY created_on DESC, seller_tin ASC LIMIT 10",
      ytQl
    )
  }

  @Test
  fun testOrderByDuplicate() {
    val ytQl = YTQLBuilder.from(document)
      .selectAll()
      .whereOr("seller_tin" to "123", "buyer_tin" to "123")
      .where("seller_tin", "string")
      .orderBy(
        "created_on" to OrderDirection.DESC,
        "created_on" to OrderDirection.DESC,
        "created_on" to OrderDirection.DESC,
        "created_on" to OrderDirection.DESC,
        "created_on" to OrderDirection.DESC,
        "created_on" to OrderDirection.DESC,
        "created_on" to OrderDirection.DESC,
        "seller_tin" to OrderDirection.ASC,
        "seller_tin" to OrderDirection.ASC,
        "seller_tin" to OrderDirection.ASC,
        "seller_tin" to OrderDirection.ASC,
        "seller_tin" to OrderDirection.ASC,
        "seller_tin" to OrderDirection.ASC,
        "seller_tin" to OrderDirection.ASC,
        "seller_tin" to OrderDirection.ASC,
      ).limit(10)
      .build()
      .query

    assertEquals(
      "* FROM [${document.name}] WHERE seller_tin = \"string\" AND (seller_tin = \"123\" OR buyer_tin = \"123\") ORDER BY created_on DESC, seller_tin ASC LIMIT 10",
      ytQl
    )
  }

  @Test
  fun testOrderByDuplicate2() {
    val ytQl = YTQLBuilder.from(document)
      .selectAll()
      .whereOr(
        "seller_tin" to "123",
        "buyer_tin" to "123"
      ).where("seller_tin", "string")
      .orderBy(
        "created_on" to OrderDirection.DESC,
        "seller_tin" to OrderDirection.ASC,
      ).limit(10)
      .build()
      .query

    assertEquals(
      "* FROM [${document.name}] WHERE seller_tin = \"string\" AND (seller_tin = \"123\" OR buyer_tin = \"123\") ORDER BY created_on DESC, seller_tin ASC LIMIT 10",
      ytQl
    )
  }

  @Test
  fun selectWithOneJoinWithUsing() {

    val ytql = YTQLJoinBuilder.from(receiptItemCode)
      .select(
        code["*"],
        receiptItemCode["id", "receipt_item_id", "code"],
      ).leftJoin(code).on(receiptItemCode["code"] eq code["code"])
      .whereIn(receiptItemCode["receipt_id"], receiptIds, string())
      .build()
      .query

    assertEquals(
      "${code.schema.columns.joinToString { "${code.simpleName}.${it.name}" }}, receipt_item_code.id, receipt_item_code.receipt_item_id, receipt_item_code.code FROM [//home/ibox-ng/rcp/receipt_item_code] AS receipt_item_code LEFT JOIN [//home/ibox/code/code] AS code ON (receipt_item_code.code) = (code.code) WHERE receipt_item_code.receipt_id IN ${
        receiptIds.joinToString(prefix = "(", postfix = ")") { "\"$it\"" }
      }",
      ytql
    )
  }

  @Test
  fun selectWithOneJoinWithOn() {

    val ytql = YTQLJoinBuilder.from(receiptItemCode)
      .select(
        code["*"],
        receiptItemCode["id", "receipt_item_id", "code"],
      ).leftJoin(code).on(receiptItemCode["code"] eq code["code"])
      .whereIn(receiptItemCode["receipt_id"], receiptIds.map { it.toString() })
      .build()
      .query

    assertEquals(
      "${code.schema.columns.joinToString { "${code.simpleName}.${it.name}" }}, receipt_item_code.id, receipt_item_code.receipt_item_id, receipt_item_code.code FROM [${receiptItemCode.name}] AS receipt_item_code LEFT JOIN [${code.name}] AS code ON (receipt_item_code.code) = (code.code) WHERE receipt_item_code.receipt_id IN ${
        receiptIds.joinToString(prefix = "(", postfix = ")") { "\"$it\"" }
      }",
      ytql
    )
  }

  @Test
  fun selectWithOneJoinWithHardOn() {

    val ytql = YTQLJoinBuilder.from(receiptItemCode)
      .select(
        code["*"],
        receiptItemCode["id", "receipt_item_id", "code"],
      ).leftJoin(code).on(
        (receiptItemCode["code"] eq code["code"]) and
            (receiptItemCode["partial_quantity"] eq code["partial_quantity"]) and
            (receiptItemCode["product_id"] eq code["product_id"])
      ).whereIn(receiptItemCode["receipt_id"], receiptIds, string())
      .build()
      .query

    assertEquals(
      "${code.schema.columns.joinToString { "${code.simpleName}.${it.name}" }}, receipt_item_code.id, receipt_item_code.receipt_item_id, receipt_item_code.code FROM [${receiptItemCode.name}] AS receipt_item_code LEFT JOIN [${code.name}] AS code ON (receipt_item_code.code, receipt_item_code.partial_quantity, receipt_item_code.product_id) = (code.code, code.partial_quantity, code.product_id) WHERE receipt_item_code.receipt_id IN ${
        receiptIds.joinToString(prefix = "(", postfix = ")") { "\"$it\"" }
      }",
      ytql
    )
  }

  @Test
  fun selectWithOneJoinWithHardOn2() {

    val ytql = YTQLJoinBuilder.from(receiptItemCode)
      .select(
        code["*"],
        receiptItemCode["id", "receipt_item_id", "code"],
      ).leftJoin(code).on(
        receiptItemCode["code", "partial_quantity", "product_id"] eq code["code", "partial_quantity", "product_id"]
      ).whereIn(receiptItemCode["receipt_id"], receiptIds, string())
      .build()
      .query

    assertEquals(
      "${code.schema.columns.joinToString { "${code.simpleName}.${it.name}" }}, receipt_item_code.id, receipt_item_code.receipt_item_id, receipt_item_code.code FROM [${receiptItemCode.name}] AS receipt_item_code LEFT JOIN [${code.name}] AS code ON (receipt_item_code.code, receipt_item_code.partial_quantity, receipt_item_code.product_id) = (code.code, code.partial_quantity, code.product_id) WHERE receipt_item_code.receipt_id IN ${
        receiptIds.joinToString(prefix = "(", postfix = ")") { "\"$it\"" }
      }",
      ytql
    )
  }

  @Test
  fun selectWithOneJoinWithHardUsing() {

    val ytql = YTQLJoinBuilder.from(receiptItemCode)
      .select(
        code["*"],
        receiptItemCode["id", "receipt_item_id", "code"],
      ).leftJoin(code).using("code", "partial_quantity", "product_id")
      .whereIn(receiptItemCode["receipt_id"], receiptIds, string())
      .build()
      .query

    assertEquals(
      "${code.schema.columns.joinToString { "${code.simpleName}.${it.name}" }}, receipt_item_code.id, receipt_item_code.receipt_item_id, receipt_item_code.code FROM [${receiptItemCode.name}] AS receipt_item_code LEFT JOIN [${code.name}] AS code ON (receipt_item_code.code, receipt_item_code.partial_quantity, receipt_item_code.product_id) = (code.code, code.partial_quantity, code.product_id) WHERE receipt_item_code.receipt_id IN ${
        receiptIds.joinToString(prefix = "(", postfix = ")") { "\"$it\"" }
      }",
      ytql
    )
  }

  @Test
  fun selectWithOneJoinWithHardUsing2() {

    val ytql = YTQLJoinBuilder.from(receiptItemCode)
      .select(
        code.allColumns,
        receiptItemCode["id", "receipt_item_id", "code"],
      ).leftJoin(code).using("code", "partial_quantity", "product_id")
      .whereIn(receiptItemCode["receipt_id"], receiptIds, string())
      .build()
      .query

    assertEquals(
      "${code.schema.columns.joinToString { "${code.simpleName}.${it.name}" }}, receipt_item_code.id, receipt_item_code.receipt_item_id, receipt_item_code.code FROM [${receiptItemCode.name}] AS receipt_item_code LEFT JOIN [${code.name}] AS code ON (receipt_item_code.code, receipt_item_code.partial_quantity, receipt_item_code.product_id) = (code.code, code.partial_quantity, code.product_id) WHERE receipt_item_code.receipt_id IN ${
        receiptIds.joinToString(prefix = "(", postfix = ")") { "\"$it\"" }
      }",
      ytql
    )
  }

  @Test
  fun selectWithTwoJoin() {

    val ytql = YTQLJoinBuilder.from(receiptItemCode)
      .select(
        code["*"],
        receiptItemCode["id", "receipt_item_id", "code"],
        receiptItem["vat_rate"]
      ).leftJoin(code).on(code["code"] eq receiptItemCode["code"])
      .innerJoin(receiptItem).on(receiptItemCode["receipt_id"] eq receiptItem["receipt_id"])
      .whereIn(receiptItemCode["receipt_id"], receiptIds.map { it.toString() })
      .build()
      .query

    assertEquals(
      "${code.schema.columns.joinToString { "${code.simpleName}.${it.name}" }}, receipt_item_code.id, receipt_item_code.receipt_item_id, receipt_item_code.code, receipt_item.vat_rate FROM [${receiptItemCode.name}] AS receipt_item_code LEFT JOIN [${code.name}] AS code ON (code.code) = (receipt_item_code.code) JOIN [${receiptItem.name}] AS receipt_item ON (receipt_item_code.receipt_id) = (receipt_item.receipt_id) WHERE receipt_item_code.receipt_id IN ${
        receiptIds.joinToString(prefix = "(", postfix = ")") { "\"$it\"" }
      }",
      ytql
    )
  }
}