package io.github.holydrug.scheme.attribute

data class TtlAttributes(
    val minDataVersions: Int?,
    val maxDataVersions: Int?,
    val minDataTtl: Int?,
    val maxDataTtl: Int?,
    val autoCompactionPeriod: Int?
)