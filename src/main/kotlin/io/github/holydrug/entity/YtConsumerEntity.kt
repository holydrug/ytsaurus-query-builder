package io.github.holydrug.entity

class YtConsumerEntity(
    val queueCluster: String,

    val queuePath: String,

    val partitionIndex: Long,

    val offset: Long? = null,

    val meta: String? = null
)