package io.github.holydrug.scheme

import io.github.holydrug.mapper.YtMapper
import io.github.holydrug.query.model.selector.column.ColumnSelector
import io.github.holydrug.scheme.attribute.TtlAttributes
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.ysontree.YTreeNode


class YtCrudTable<T>(
    path: YPath,
    schema: TableSchema,
    mapper: YtMapper<T>,
    optimizeFor: String = "lookup",
    specificAttributes: Map<String, YTreeNode> = emptyMap(),
    ttlAttributes: TtlAttributes? = null
) : AbstractYtTable<T>(path, schema, mapper, optimizeFor, specificAttributes, ttlAttributes) {

    val allColumns = ColumnSelector(this, setOf("*"), selectAll = true)

    fun insert(vararg r: T) = modify().apply { r.forEach { addInsert(mapper.toYtMap(it)) } }.build()

    fun update(vararg r: T) = modify().apply { r.forEach { addUpdate(mapper.toYtMap(it)) } }.build()

    fun delete(r: T) = modify().addDelete(mapper.toYtMap(r)).build()

    fun delete(vararg keys: List<*>) = modify().addDeletes(keys.toList()).build()

    operator fun get(vararg columnName: String) = ColumnSelector(
        this,
        columnName.toSet(),
        selectAll = "*" in columnName
    )
}