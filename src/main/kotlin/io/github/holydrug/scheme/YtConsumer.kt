package io.github.holydrug.scheme

import io.github.holydrug.entity.YtConsumerEntity
import io.github.holydrug.mapper.YtMapper
import io.github.holydrug.mapper.impl.YtConsumerMapper
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.ColumnSchema
import tech.ytsaurus.core.tables.ColumnSortOrder
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.typeinfo.TiType

class YtConsumer(
    path: YPath,
    schema: TableSchema = TableSchema.builder().setUniqueKeys(false).setStrict(true)
        .add(ColumnSchema("queue_cluster", TiType.string(), ColumnSortOrder.ASCENDING))
        .add(ColumnSchema("queue_path", TiType.string(), ColumnSortOrder.ASCENDING))
        .add(ColumnSchema("partition_index", TiType.uint64(), ColumnSortOrder.ASCENDING))
        .add(ColumnSchema("offset", TiType.uint64()))
        .add(ColumnSchema("meta", TiType.optional(TiType.yson())))
        .build(),
    mapper: YtMapper<YtConsumerEntity> = YtConsumerMapper
) : AbstractYtTable<YtConsumerEntity>(path, schema, mapper)