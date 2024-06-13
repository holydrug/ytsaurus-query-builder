package io.github.holydrug.query.builder

import io.github.holydrug.query.model.column.ColumnSelector
import io.github.holydrug.query.model.join.YTQLJoin
import io.github.holydrug.query.model.join.YTQLJoinCondition
import io.github.holydrug.query.model.join.YTQLJoinType
import io.github.holydrug.query.model.order.OrderDirection
import io.github.holydrug.scheme.YtCrudTable
import tech.ytsaurus.client.request.SelectRowsRequest
import tech.ytsaurus.typeinfo.TiType

/**
 * The `YTQLJoinBuilder` class is responsible for constructing SQL-like queries with support for join operations on YT tables.
 *
 * @param LHS the type of the main table's entity.
 * @property mainTable the main table on which the queries are built.
 */
class YTQLJoinBuilder<LHS : Any>(mainTable: YtCrudTable<LHS>) : AbstractYTQLBuilder<LHS>(mainTable) {

    private val joins: MutableSet<YTQLJoin<out Any>> = mutableSetOf()

    companion object {

        /**
         * Creates a new instance of `YTQLJoinBuilder` for the specified main table.
         *
         * @param LHS the type of the main table's entity.
         * @param mainTable the main table on which the queries are built.
         * @return a new instance of `YTQLJoinBuilder`.
         */
        @JvmStatic
        fun <LHS : Any> from(mainTable: YtCrudTable<LHS>) = YTQLJoinBuilder(mainTable)
    }

    /**
     * Selects the specified columns in the query.
     *
     * @param columns the columns to select.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun select(vararg columns: ColumnSelector<out Any>) = apply {
        selectStatement = selectFrom(
            buildString {
                columns.forEachIndexed { index, columnSelector ->
                    if (columnSelector.selectAll)
                        append(columnSelector.table.schema.columns.joinToString { "${columnSelector.table.simpleName}.${it.name}" })
                    else append(columnSelector.columns.joinToString { "${columnSelector.table.simpleName}.$it" })
                    appendComma(index, *columns)
                }
            },
            mainTable.simpleName
        )
    }

    /**
     * Adds an inner join to the query.
     *
     * @param RHS the type of the joined table's entity.
     * @param tableToJoin the table to join.
     * @return a `JoinBuilder` for further configuration of the join.
     */
    fun <RHS : Any> innerJoin(tableToJoin: YtCrudTable<RHS>) = JoinBuilder(this, YTQLJoinType.INNER, tableToJoin)

    /**
     * Adds a left join to the query.
     *
     * @param RHS the type of the joined table's entity.
     * @param tableToJoin the table to join.
     * @return a `JoinBuilder` for further configuration of the join.
     */
    fun <RHS : Any> leftJoin(tableToJoin: YtCrudTable<RHS>) = JoinBuilder(this, YTQLJoinType.LEFT, tableToJoin)

    /**
     * Adds a where condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the column selector for the condition.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun <T : Any> where(
        name: ColumnSelector<out Any>,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhere(name.singleFullName, value, ytType) as YTQLJoinBuilder

    /**
     * Adds a where-not condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the column selector for the condition.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun <T : Any> whereNot(
        name: ColumnSelector<out Any>,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhereNot(name.singleFullName, value, ytType) as YTQLJoinBuilder

    /**
     * Adds a where-not-null condition to the query.
     *
     * @param name the column selector for the condition.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun whereNotNull(name: ColumnSelector<out Any>) = protectedWhereNotNull(name.singleFullName) as YTQLJoinBuilder

    /**
     * Adds a where-in condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the column selector for the condition.
     * @param value the collection of values to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun <T : Any> whereIn(
        name: ColumnSelector<out Any>,
        value: Collection<T>,
        ytType: TiType? = null,
    ) = protectedWhereIn(name.singleFullName, value, ytType) as YTQLJoinBuilder

    /**
     * Adds a where-or condition to the query.
     *
     * @param T the type of the value being compared.
     * @param firstNameAndValue the first column selector and value pair.
     * @param secondNameAndValue the second column selector and value pair.
     * @param firstYtType the YT type information for the first column.
     * @param secondYtType the YT type information for the second column.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun <T : Any> whereOr(
        firstNameAndValue: Pair<ColumnSelector<out Any>, T>,
        secondNameAndValue: Pair<ColumnSelector<out Any>, T>,
        firstYtType: TiType? = null,
        secondYtType: TiType? = null,
    ) = protectedWhereOr(
        firstNameAndValue = firstNameAndValue.let { (selector, value) -> selector.singleFullName to value },
        secondNameAndValue = secondNameAndValue.let { (selector, value) -> selector.singleFullName to value },
        firstYtType = firstYtType,
        secondYtType = secondYtType
    ) as YTQLJoinBuilder

    /**
     * Adds a where-less condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the column selector for the condition.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun <T : Any> whereLess(
        name: ColumnSelector<out Any>,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhereLess(name.singleFullName, value, ytType) as YTQLJoinBuilder

    /**
     * Adds a where-greater condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the column selector for the condition.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun <T : Any> whereGreater(
        name: ColumnSelector<out Any>,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhereGreater(name.singleFullName, value, ytType) as YTQLJoinBuilder

    /**
     * Adds an order by clause to the query.
     *
     * @param nameWithDirection the column selectors with their respective order directions.
     * @return an `OrderByWithJoinBuilder` for further configuration of the order by clause.
     */
    fun orderBy(
        vararg nameWithDirection: Pair<ColumnSelector<out Any>, OrderDirection>
    ) = OrderByWithJoinBuilder(this, nameWithDirection.toSet())

    /**
     * Sets the limit for the number of rows to return.
     *
     * @param value the limit value.
     * @return the current `YTQLJoinBuilder` instance.
     */
    fun limit(value: Int) = protectedLimit(value) as YTQLJoinBuilder

    /**
     * Builds the final `SelectRowsRequest` for the query.
     *
     * @return the constructed `SelectRowsRequest`.
     */
    override fun build(): SelectRowsRequest {
        if (selectStatement == null) select(mainTable.allColumns)
        return buildRequest { appendJoins() }
    }

    private fun <RHS : Any> buildJoin(
        type: YTQLJoinType,
        tableToJoin: YtCrudTable<RHS>,
        condition: YTQLJoinCondition
    ) = apply { joins.add(YTQLJoin(type, tableToJoin, condition)) }

    private fun StringBuilder.appendComma(index: Int, vararg current: Any) {
        if (index < current.size - 1) append(", ")
    }

    private fun StringBuilder.appendJoins() =
        joins.forEach { join -> append(" ${join.type.value} ${table(join)} ON ${condition(join)}") }

    private fun <T> condition(join: YTQLJoin<T>) = "${
        join.condition.firstJoinColumns.joinToString(prefix = "(", postfix = ")")
    } = ${join.condition.secondJoinColumns.joinToString(prefix = "(", postfix = ")")}"

    private fun <T> table(join: YTQLJoin<T>) = "[${join.tableToJoin.name}] AS ${join.tableToJoin.simpleName}"

    /**
     * The `JoinBuilder` class assists in constructing join conditions.
     *
     * @param LHS the type of the main table's entity.
     * @param RHS the type of the joined table's entity.
     * @property queryBuilder the main `YTQLJoinBuilder` instance.
     * @property joinType the type of join (INNER, LEFT, etc.).
     * @property tableToJoin the table to join.
     */
    class JoinBuilder<LHS : Any, RHS : Any>(
        private val queryBuilder: YTQLJoinBuilder<LHS>,
        private val joinType: YTQLJoinType,
        private val tableToJoin: YtCrudTable<RHS>
    ) {

        /**
         * Specifies the join condition.
         *
         * @param condition the join condition.
         * @return the main `YTQLJoinBuilder` instance.
         */
        fun on(condition: YTQLJoinCondition) = queryBuilder.buildJoin(joinType, tableToJoin, condition)

        /**
         * Specifies the join condition using the specified keys.
         *
         * @param key the keys to use for the join condition.
         * @return the main `YTQLJoinBuilder` instance.
         */
        fun using(vararg key: String) = queryBuilder.buildJoin(
            joinType,
            tableToJoin,
            condition = queryBuilder.mainTable.get(*key) eq tableToJoin.get(*key)
        )
    }

    /**
     * The `OrderByWithJoinBuilder` class assists in constructing order by conditions.
     *
     * @param MT the type of the main table's entity.
     * @property queryBuilder the main `YTQLJoinBuilder` instance.
     * @property selectorsWithDirection the columns with their respective order directions.
     */
    class OrderByWithJoinBuilder<MT : Any>(
        private val queryBuilder: YTQLJoinBuilder<MT>,
        private val selectorsWithDirection: Set<Pair<ColumnSelector<out Any>, OrderDirection>>
    ) : AbstractOrderByBuilder<MT> {

        /**
         * Sets the limit for the number of rows to return.
         *
         * @param value the limit value.
         * @return the main `YTQLJoinBuilder` instance.
         */
        override fun limit(value: Int) = queryBuilder.apply {
            buildOrderBy(selectorsWithDirection.mapTo(mutableSetOf()) { (selector, direction) -> selector.singleFullName to direction })
            limit(value)
        }
    }
}