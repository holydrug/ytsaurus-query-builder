package io.github.holydrug.scheme

import tech.ytsaurus.core.tables.ColumnSchema
import tech.ytsaurus.typeinfo.OptionalType
import tech.ytsaurus.ysontree.YTree

/** Колонка в таблице со схемой и порядковым номером */
interface YtColumn {
    val schema: ColumnSchema
    val ordinal: Int

    fun <T> asPair(value: T): Pair<String, Any> {
        if (value == null && isRequired()) {
            error("${schema.name} is required!")
        }
        return schema.name to (value ?: YTree.nullNode())
    }

    fun isRequired() = schema.typeV3 !is OptionalType
}


