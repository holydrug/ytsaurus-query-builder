package io.github.holydrug.mapper.extension

import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.typeinfo.TiType.datetime
import tech.ytsaurus.typeinfo.TiType.timestamp
import tech.ytsaurus.ysontree.YTree
import tech.ytsaurus.ysontree.YTreeListNode
import tech.ytsaurus.ysontree.YTreeMapNode
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
Дополнительные типы
 **/
fun YTreeMapNode.getFloatNullable(key: String): Float? = getDoubleNullable(key)?.toFloat()

fun YTreeMapNode.getFloat(key: String): Float = getDouble(key).toFloat()

fun YTreeMapNode.getInstantNullable(
    key: String,
    from: TiType,
    truncateTo: ChronoUnit = when (from) {
        timestamp() -> ChronoUnit.MICROS
        datetime() -> ChronoUnit.SECONDS
        else -> throw IllegalArgumentException("Not allowed source type")
    }
): Instant? = try {
    getInstant(key, from, truncateTo)
} catch (e: IllegalStateException) {
    null
}

fun YTreeMapNode.getInstant(
    key: String,
    from: TiType,
    truncateTo: ChronoUnit = when (from) {
        timestamp() -> ChronoUnit.MICROS
        datetime() -> ChronoUnit.SECONDS
        else -> throw IllegalArgumentException("Not allowed source type")
    }
): Instant {
    when (from) {
        timestamp() -> require(truncateTo != ChronoUnit.NANOS) { "Nanos precision is not accurate for micros-based Instant" }
        datetime() -> require(
            truncateTo !in listOf(
                ChronoUnit.NANOS,
                ChronoUnit.MICROS,
                ChronoUnit.MILLIS
            )
        ) { "Time precision must be limited to seconds. Usage of nanos, micros, or millis for truncation is not allowed" }
    }
    return getLongO(key)
        .map {
            when (from) {
                timestamp() -> it.toInstantFromMicro(truncateTo)
                datetime() -> Instant.ofEpochSecond(it).truncatedTo(truncateTo)
                else -> throw IllegalArgumentException("Not allowed source type")
            }
        }
        .orElseThrow { IllegalStateException("Value for key '$key' is null") }
}

/**
Преобразование optional в nullable
 **/
fun YTreeMapNode.getStringNullable(key: String): String? = getStringO(key).orElse(null)

fun YTreeMapNode.getDoubleNullable(key: String): Double? = getDoubleO(key).orElse(null)

fun YTreeMapNode.getBoolNullable(key: String): Boolean? = getBoolO(key).orElse(null)

fun YTreeMapNode.getIntNullable(key: String): Int? = getIntO(key).orElse(null)

fun YTreeMapNode.getBytesNullable(key: String): ByteArray = getBytesO(key).orElse(null)

fun YTreeMapNode.getLongNullable(key: String): Long? = getLongO(key).orElse(null)

fun YTreeMapNode.getListNullable(key: String): YTreeListNode? = getListO(key).orElse(null)

fun YTreeMapNode.getMapNullable(key: String): YTreeMapNode = getMapO(key).orElse(null)

fun List<String>?.toListNode() = if (isNullOrEmpty()) null else YTree.builder().value(this).build()