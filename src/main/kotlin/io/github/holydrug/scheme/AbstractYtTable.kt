package io.github.holydrug.scheme

import io.github.holydrug.mapper.YtMapper
import io.github.holydrug.scheme.attribute.TtlAttributes
import tech.ytsaurus.client.request.LookupRowsRequest
import tech.ytsaurus.client.request.ModifyRowsRequest
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.ysontree.YTreeNode


abstract class AbstractYtTable<T>(
    val path: YPath,
    var schema: TableSchema,
    val mapper: YtMapper<T>,
    val optimizeFor: String = "lookup",
    val specificAttributes: Map<String, YTreeNode> = emptyMap(),
    val ttlAttributes: TtlAttributes? = null
) {

    val name by lazy { path.toString() }

    val simpleName = name.substringAfterLast('/')

    // без вычисляемых
    val writeSchema: TableSchema by lazy { schema.toWrite() }

    // ключевые без вычисляемых
    val lookupSchema: TableSchema by lazy { schema.toLookup() }

    val keys = List(lookupSchema.keyColumnsCount) { n -> lookupSchema.getColumnName(n) }

    fun lookup() = LookupRowsRequest.builder()
        .setPath(name)
        .setSchema(lookupSchema)

    fun modify() = ModifyRowsRequest.builder()
        .setPath(name)
        .setSchema(writeSchema)

    fun schemaWithoutColumns(columns: List<String>) = schema.toBuilder()
        .setColumns(schema.columns.filter { it.name !in columns })
        .build()
}