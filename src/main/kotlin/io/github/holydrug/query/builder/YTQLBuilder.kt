package io.github.holydrug.query.builder

import io.github.holydrug.query.model.order.OrderDirection
import io.github.holydrug.query.model.param.YTQLListParam
import io.github.holydrug.query.model.param.YTQLOrParam
import io.github.holydrug.query.model.param.YTQLParam
import io.github.holydrug.query.model.selector.YtSelector
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

  private var groupByStatement: String? = null
  private val havingParams: MutableSet<YTQLParam<out Any>> = mutableSetOf()
  private val havingOrParams: MutableSet<YTQLOrParam<out Any, out Any>> = mutableSetOf()
  private val havingNotNullParams: MutableSet<YTQLParam<Unit>> = mutableSetOf()
  private val havingListParams: MutableSet<YTQLListParam<out Any>> = mutableSetOf()

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
   * Selects the specified columns and aggregate fun's in the query.
   *
   * @param params the columns and aggregate fun's to select.
   * @return the current `YTQLBuilder` instance.
   */
  fun select(vararg params: YtSelector) = apply {
    selectStatement = selectFrom(params.joinToString { it.value })
  }

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
  ) = equalsCondition(name, value, ytType) as YTQLBuilder

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
  ) = notEqualsCondition(name, value, ytType) as YTQLBuilder

  /**
   * Adds a where-not-null condition to the query.
   *
   * @param name the name of the column.
   * @return the current `YTQLBuilder` instance.
   */
  fun whereNotNull(name: String) = notNullCondition(name) as YTQLBuilder

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
    value: Iterable<T>,
    ytType: TiType? = null,
  ) = inCondition(name, value, ytType) as YTQLBuilder

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
  ) = orCondition(firstNameAndValue, secondNameAndValue, firstYtType, secondYtType) as YTQLBuilder

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
  ) = lessCondition(name, value, ytType) as YTQLBuilder

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
  ) = greaterCondition(name, value, ytType) as YTQLBuilder

  /**
   * Adds an order by clause to the query.
   *
   * @param nameWithDirection the column names with their respective order directions.
   * @return an `OrderBySingleBuilder` for further configuration of the order by clause.
   */
  fun orderBy(vararg nameWithDirection: Pair<String, OrderDirection>) = OrderByBuilder(nameWithDirection.toSet())

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
    return buildRequest(groupByAction = { appendGroupBy() })
  }

  /**
   * Adds a group by clause to the query.
   *
   * @param params the columns to group by.
   * @return the ` GroupByBuilder` instance.
   */
  fun groupBy(vararg params: String) = GroupByBuilder()
    .also { groupByStatement = " GROUP BY ${params.joinToString { it }}" }

  private fun StringBuilder.appendGroupBy() = groupByStatement?.let {
    append(groupByStatement)
    if (hasParams(havingParams, havingNotNullParams, havingListParams, havingOrParams)) append(" HAVING ")
    appendCondition(havingParams, havingNotNullParams, havingListParams, havingOrParams)
  }

  inner class GroupByBuilder {

    /**
     * Adds a having condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `GroupByBuilder` instance.
     */
    fun <T : Any> having(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = apply { equalsCondition(name, value, ytType, havingParams) }

    /**
     * Adds a having-not condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `GroupByBuilder` instance.
     */
    fun <T : Any> havingNot(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = apply { notEqualsCondition(name, value, ytType, havingParams) }

    /**
     * Adds a having-not-null condition to the query.
     *
     * @param name the name of the column.
     * @return the current `GroupByBuilder` instance.
     */
    fun havingNotNull(name: String) = apply { notNullCondition(name, havingNotNullParams) }

    /**
     * Adds a having-in condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the collection of values to compare.
     * @param ytType the YT type information.
     * @return the current `GroupByBuilder` instance.
     */
    fun <T : Any> havingIn(
      name: String,
      value: Iterable<T>,
      ytType: TiType? = null,
    ) = apply { inCondition(name, value, ytType, havingListParams) }

    /**
     * Adds a having-or condition to the query.
     *
     * @param T the type of the value being compared.
     * @param firstNameAndValue the first column name and value pair.
     * @param secondNameAndValue the second column name and value pair.
     * @param firstYtType the YT type information for the first column.
     * @param secondYtType the YT type information for the second column.
     * @return the current `GroupByBuilder` instance.
     */
    fun <T : Any> havingOr(
      firstNameAndValue: Pair<String, T>,
      secondNameAndValue: Pair<String, T>,
      firstYtType: TiType? = null,
      secondYtType: TiType? = null,
    ) = apply { orCondition(firstNameAndValue, secondNameAndValue, firstYtType, secondYtType, havingOrParams) }

    /**
     * Adds a having-less condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `GroupByBuilder` instance.
     */
    fun <T : Any> havingLess(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = apply { lessCondition(name, value, ytType, havingParams) }

    /**
     * Adds a having-greater condition to the query.
     *
     * @param T the type of the value being compared.
     * @param name the name of the column.
     * @param value the value to compare.
     * @param ytType the YT type information.
     * @return the current `GroupByBuilder` instance.
     */
    fun <T : Any> havingGreater(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = apply { greaterCondition(name, value, ytType, havingParams) }

    fun <T : Any> where(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = this@YTQLBuilder.where(name, value, ytType)

    fun <T : Any> whereNot(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = this@YTQLBuilder.whereNot(name, value, ytType)

    fun whereNotNull(name: String) = this@YTQLBuilder.whereNotNull(name)

    fun <T : Any> whereIn(
      name: String,
      value: Iterable<T>,
      ytType: TiType? = null,
    ) = this@YTQLBuilder.whereIn(name, value, ytType)

    fun <T : Any> whereOr(
      firstNameAndValue: Pair<String, T>,
      secondNameAndValue: Pair<String, T>,
      firstYtType: TiType? = null,
      secondYtType: TiType? = null,
    ) = this@YTQLBuilder.whereOr(firstNameAndValue, secondNameAndValue, firstYtType, secondYtType)

    fun <T : Any> whereLess(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = this@YTQLBuilder.whereLess(name, value, ytType)

    fun <T : Any> whereGreater(
      name: String,
      value: T,
      ytType: TiType? = null,
    ) = this@YTQLBuilder.whereGreater(name, value, ytType)

    fun orderBy(vararg nameWithDirection: Pair<String, OrderDirection>) = this@YTQLBuilder.orderBy(*nameWithDirection)

    fun limit(value: Int) = this@YTQLBuilder.limit(value)

    fun build() = this@YTQLBuilder.build()
  }

  /**
   * The `OrderBySingleBuilder` class assists in constructing order by conditions.
   *
   * @param MT the type of the main table's entity.
   * @property namesWithDirection the columns with their respective order directions.
   */
  inner class OrderByBuilder(
    private val namesWithDirection: Set<Pair<String, OrderDirection>>
  ) : AbstractOrderByBuilder<MT> {

    /**
     * Sets the limit for the number of rows to return.
     *
     * @param value the limit value.
     * @return the main `YTQLBuilder` instance.
     */
    override fun limit(value: Int) = this@YTQLBuilder.apply {
      buildOrderBy(namesWithDirection)
      limit(value)
    }
  }
}