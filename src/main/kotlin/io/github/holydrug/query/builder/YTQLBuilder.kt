package io.github.holydrug.query.builder

import io.github.holydrug.query.model.order.OrderDirection
import io.github.holydrug.scheme.YtCrudTable
import tech.ytsaurus.client.request.SelectRowsRequest
import tech.ytsaurus.typeinfo.TiType

/**
 * The `YTQLBuilder` class is responsible for constructing SQL-like queries on YT tables.
 *
 * @param MT the type of the main table's entity.
 * @property mainTable the main table on which the queries are built.
 */
class YTQLBuilder<MT>(mainTable: YtCrudTable<MT>) : AbstractYTQLBuilder<MT>(mainTable) {

    companion object {

        /**
         * Creates a new instance of `YTQLBuilder` for the specified main table.
         *
         * @param MT the type of the main table's entity.
         * @param mainTable the main table on which the queries are built.
         * @return a new instance of `YTQLBuilder`.
         */
        @JvmStatic
        fun <MT> from(mainTable: YtCrudTable<MT>) = YTQLBuilder(mainTable)
    }

    /**
     * Selects all columns in the query.
     *
     * @return the current `YTQLBuilder` instance.
     */
    fun selectAll() = apply { selectStatement = selectFrom("*") }

    /**
     * Selects the specified columns in the query.
     *
     * @param params the columns to select.
     * @return the current `YTQLBuilder` instance.
     */
    fun select(vararg params: String) = apply { selectStatement = selectFrom(params.joinToString { it }) }

    /**
     * Adds a where condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLBuilder` instance.
     */
    fun <T : Any> where(
        name: String,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhere(name, value, ytType) as YTQLBuilder

    /**
     * Adds a where-not condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLBuilder` instance.
     */
    fun <T : Any> whereNot(
        name: String,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhereNot(name, value, ytType) as YTQLBuilder

    /**
     * Adds a where-not-null condition to the query.
     *
     * @param name the name of the column.
     * @return the current `YTQLBuilder` instance.
     */
    fun whereNotNull(name: String) = protectedWhereNotNull(name) as YTQLBuilder

    /**
     * Adds a where-in condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the collection of values to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLBuilder` instance.
     */
    fun <T : Any> whereIn(
        name: String,
        value: Collection<T>,
        ytType: TiType? = null,
    ) = protectedWhereIn(name, value, ytType) as YTQLBuilder

    /**
     * Adds a where-or condition to the query.
     *
     * @param T the type of the value being compared.
     * @param firstNameAndValue the first column name and value pair.
     * @param secondNameAndValue the second column name and value pair.
     * @param firstYtType the YT type information for the first column.
     * @param secondYtType the YT type information for the second column.
     * @return the current `YTQLBuilder` instance.
     */
    fun <T : Any> whereOr(
        firstNameAndValue: Pair<String, T>,
        secondNameAndValue: Pair<String, T>,
        firstYtType: TiType? = null,
        secondYtType: TiType? = null,
    ) = protectedWhereOr(firstNameAndValue, secondNameAndValue, firstYtType, secondYtType) as YTQLBuilder

    /**
     * Adds a where-less condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLBuilder` instance.
     */
    fun <T : Any> whereLess(
        name: String,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhereLess(name, value, ytType) as YTQLBuilder

    /**
     * Adds a where-greater condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `YTQLBuilder` instance.
     */
    fun <T : Any> whereGreater(
        name: String,
        value: T,
        ytType: TiType? = null,
    ) = protectedWhereGreater(name, value, ytType) as YTQLBuilder

    /**
     * Adds an order by clause to the query.
     *
     * @param nameWithDirection the column names with their respective order directions.
     * @return an `OrderBySingleBuilder` for further configuration of the order by clause.
     */
    fun orderBy(vararg nameWithDirection: Pair<String, OrderDirection>) =
        OrderByBuilder(this, nameWithDirection.toSet())

    /**
     * Sets the limit for the number of rows to return.
     *
     * @param value the limit value.
     * @return the current `YTQLBuilder` instance.
     */
    fun limit(value: Int) = protectedLimit(value) as YTQLBuilder

    /**
     * Builds the final `SelectRowsRequest` for the query.
     *
     * @return the constructed `SelectRowsRequest`.
     */
    override fun build(): SelectRowsRequest {
        if (selectStatement == null) selectAll()
        return buildRequest { }
    }

    /**
     * The `OrderBySingleBuilder` class assists in constructing order by conditions.
     *
     * @param MT the type of the main table's entity.
     * @property queryBuilder the main `YTQLBuilder` instance.
     * @property namesWithDirection the columns with their respective order directions.
     */
    class OrderByBuilder<MT>(
        private val queryBuilder: YTQLBuilder<MT>,
        private val namesWithDirection: Set<Pair<String, OrderDirection>>
    ) : AbstractOrderByBuilder<MT> {

        /**
         * Sets the limit for the number of rows to return.
         *
         * @param value the limit value.
         * @return the main `YTQLBuilder` instance.
         */
        override fun limit(value: Int) = queryBuilder.apply {
            buildOrderBy(namesWithDirection)
            limit(value)
        }
    }
}