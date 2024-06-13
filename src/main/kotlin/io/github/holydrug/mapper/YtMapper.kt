package io.github.holydrug.mapper

import tech.ytsaurus.ysontree.YTreeMapNode

interface YtMapper<T> {
    fun toYtMap(r: T): Map<String, Any?>

    fun fromYt(r: YTreeMapNode): T
}