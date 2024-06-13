package io.github.holydrug.mapper.impl

import io.github.holydrug.entity.YtConsumerEntity
import io.github.holydrug.mapper.YtMapper
import io.github.holydrug.mapper.extension.getLongNullable
import io.github.holydrug.mapper.extension.getStringNullable
import tech.ytsaurus.ysontree.YTreeMapNode

object YtConsumerMapper : YtMapper<YtConsumerEntity> {

    override fun toYtMap(r: YtConsumerEntity): Map<String, Any?> = mapOf(
        "queue_cluster" to r.queueCluster,
        "queue_path" to r.queuePath,
        "partition_index" to r.partitionIndex,
        "offset" to r.offset,
        "meta" to r.meta
    )

    override fun fromYt(r: YTreeMapNode): YtConsumerEntity = YtConsumerEntity(
        queueCluster = r.getString("queue_cluster"),
        queuePath = r.getString("queue_path"),
        partitionIndex = r.getLong("partition_index"),
        offset = r.getLongNullable("offset"),
        meta = r.getStringNullable("meta")
    )
}