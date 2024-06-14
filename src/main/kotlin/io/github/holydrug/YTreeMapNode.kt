package io.github.holydrug

import io.github.holydrug.scheme.YtColumn
import tech.ytsaurus.ysontree.YTreeIntegerNode
import tech.ytsaurus.ysontree.YTreeListNode
import tech.ytsaurus.ysontree.YTreeMapNode
import tech.ytsaurus.ysontree.YTreeMapNodeImpl
import tech.ytsaurus.ysontree.YTreeStringNode
import tech.ytsaurus.ysontree.YTreeStringNodeImpl

fun YTreeMapNode.opString(name: String): String? {
    val op = get(name)
    if (op != null && op.isPresent) {
        val node = op.get()
        if (node is YTreeStringNode)
            return node.value
    }
    return null
}

fun YTreeMapNode.opLong(name: String): Long? {
    val op = get(name)
    if (op != null && op.isPresent) {
        val node = op.get()
        if (node is YTreeIntegerNode)
            return node.long
    }
    return null
}
fun YTreeMapNode.opInt(name: String): Int? {
    val op = get(name)
    if (op != null && op.isPresent) {
        val node = op.get()
        if (node is YTreeIntegerNode)
            return node.int
    }
    return null
}

fun YTreeMapNode.opList(name: String): YTreeListNode? {
    val op = get(name)
    if (op != null && op.isPresent) {
        val node = op.get()
        if (node is YTreeListNode) {
            return node
        }
    }
    return null
}

fun YTreeMapNode.opMap(name: String): YTreeMapNode? {
    val op = get(name)
    if (op != null && op.isPresent) {
        val node = op.get()
        if (node is YTreeMapNode) {
            return node
        }
    }
    return null
}

fun YTreeMapNode.getString(column: YtColumn): String = this.getString(column.schema.name)
fun YTreeMapNode.getInt(column: YtColumn): Int = this.getInt(column.schema.name)
fun YTreeMapNode.opString(column: YtColumn): String? = this.opString(column.schema.name)
fun YTreeMapNode.opInt(column: YtColumn): Int? = this.opInt(column.schema.name)
fun YTreeMapNode.getLong(column: YtColumn): Long = this.getLong(column.schema.name)
fun YTreeMapNode.opLong(column: YtColumn): Long? = this.opLong(column.schema.name)
fun YTreeMapNode.opList(column: YtColumn): YTreeListNode? = this.opList(column.schema.name)
fun YTreeMapNode.toMap(): Map<String, String> = this.keys().associateWith { this.getString(it) }
fun Map<String, String>.toYTreeMap() = this.entries
  .associate { (k, v) -> k to YTreeStringNodeImpl(v, null) }
  .let { YTreeMapNodeImpl(it, null) }