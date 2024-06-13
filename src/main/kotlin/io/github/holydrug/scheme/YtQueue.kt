package io.github.holydrug.scheme

import io.github.holydrug.mapper.YtMapper
import io.github.holydrug.scheme.attribute.TtlAttributes
import tech.ytsaurus.client.rows.UnversionedRow
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.TableSchema

class YtQueue<T>(
    path: YPath,
    schema: TableSchema,
    val toYTreeMapSchema: TableSchema,
    mapper: YtMapper<T>,
    optimizeFor: String = "lookup",
    ttlAttributes: TtlAttributes? = null
) : AbstractYtTable<T>(path, schema, mapper, optimizeFor, emptyMap(), ttlAttributes) {

    fun insert(r: T) = modify().addInsert(mapper.toYtMap(r)).build()

    fun rowToEntity(row: UnversionedRow): T = mapper.fromYt(
        row.toYTreeMap(toYTreeMapSchema)
    )
}