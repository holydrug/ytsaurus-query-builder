package io.github.holydrug.query.model.column

import io.github.holydrug.query.model.join.YTQLJoinCondition
import io.github.holydrug.scheme.YtCrudTable

/**
 * Represents a selector for columns in a YT CRUD table.
 *
 * @param LHS The type of the entity associated with the table.
 * @property table The YT CRUD table associated with the column selector.
 * @property columns The set of column names to be selected.
 * @property selectAll Indicates whether all columns should be selected. Defaults to false.
 */
data class ColumnSelector<LHS>(
    val table: YtCrudTable<LHS>,
    val columns: Set<String>,
    val selectAll: Boolean = false
) {

    /**
     * The name of the single column selected.
     */
    val singleName = columns.first()

    /**
     * The full name of the single selected column, including the table name prefix.
     */
    val singleFullName = "${table.simpleName}.$singleName"

    /**
     * The full names of all selected columns, including the table name prefix.
     */
    val allFullColumnNames = columns.mapTo(mutableSetOf()) { "${table.simpleName}.$it" }

    /**
     * Creates a join condition between this column selector and another column selector.
     *
     * @param RHS The type of the entity associated with the other table.
     * @param other The other column selector to join with.
     * @return A YTQLJoinCondition representing the join condition.
     */
    infix fun <RHS> eq(other: ColumnSelector<RHS>) = YTQLJoinCondition(allFullColumnNames, other.allFullColumnNames)
}