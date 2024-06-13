package io.github.holydrug.query.model.join


data class YTQLJoinCondition(
    val firstJoinColumns: MutableSet<String>,
    val secondJoinColumns: MutableSet<String>
) {

    infix fun and(
        other: YTQLJoinCondition
    ) = apply {
        firstJoinColumns.addAll(other.firstJoinColumns)
        secondJoinColumns.addAll(other.secondJoinColumns)
    }
}