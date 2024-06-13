package io.github.holydrug.mapper.impl

import io.github.holydrug.mapper.YtMapper
import tech.ytsaurus.ysontree.YTreeMapNode

object NoYtMapper : YtMapper<Unit> {
    override fun toYtMap(r: Unit): Map<String, Any?> = mapOf()

    override fun fromYt(r: YTreeMapNode) {
    }
}