package io.github.holydrug.query.builder

import io.github.holydrug.mapper.extension.toEpochMicro
import io.github.holydrug.query.model.order.OrderDirection
import io.github.holydrug.query.model.param.YTQLListParam
import io.github.holydrug.query.model.param.YTQLOrParam
import io.github.holydrug.query.model.param.YTQLParam
import io.github.holydrug.query.model.param.YTQLParamOperator
import io.github.holydrug.scheme.YtCrudTable
import tech.ytsaurus.client.request.SelectRowsRequest
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.typeinfo.TiType.datetime
import tech.ytsaurus.typeinfo.TiType.timestamp
import java.time.Instant

/**
 * The `AbstractYTQLBuilder` class provides a foundation for building SQL-like queries on YT tables, with support for various conditions and operations.
 *
 * @param MT the type of the main table's entity.
 * @property mainTable the main table on which the queries are built.
 * @property selectStatement the SELECT statement of the query.
 * @property whereParams the set of parameters for the query.
 * @property whereOrParams the set of OR conditions for the query.
 * @property whereNotNullParams the set of IS NOT NULL conditions for the query.
 * @property whereListParams the set of IN conditions for the query.
 * @property orderByStatement the ORDER BY statement of the query.
 * @property limit the limit for the number of rows to return.
 */
abstract class AbstractYTQLBuilder<MT> protected constructor(
  val mainTable: YtCrudTable<MT>,
  protected var selectStatement: String? = null,
  private val whereParams: MutableSet<YTQLParam<out Any>> = mutableSetOf(),
  private val whereOrParams: MutableSet<YTQLOrParam<out Any, out Any>> = mutableSetOf(),
  private val whereNotNullParams: MutableSet<YTQLParam<Unit>> = mutableSetOf(),
  private val whereListParams: MutableSet<YTQLListParam<out Any>> = mutableSetOf(),
  private var orderByStatement: String? = null,
  private var limit: Int? = null
) {

  /**
   * Abstract method to build the final `SelectRowsRequest` for the query.
   *
   * @return the constructed `SelectRowsRequest`.
   */
  abstract fun build(): SelectRowsRequest

  /**
   * Adds an eq-condition to the query.
   *
   * @param T the type of the value being compared.
   * @param name the name of the column.
   * @param value the value to compare.
   * @param ytType the YT type information.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun <T : Any> equalsCondition(
    name: String,
    value: T,
    ytType: TiType? = null,
    havingParams: MutableSet<YTQLParam<out Any>>? = null,
  ) = apply {
    when (value) {
      is Instant -> {
        require(ytType != null) { "For instant type yt type have to be not null" }
        (havingParams ?: whereParams).add(instantParam(name, value = value, ytType = ytType))
      }

      else -> (havingParams ?: whereParams).add(YTQLParam(name, value = value, ytType = ytType))
    }
  }

  /**
   * Adds a not-eq-condition to the query.
   *
   * @param T the type of the value being compared.
   * @param name the name of the column.
   * @param value the value to compare.
   * @param ytType the YT type information.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun <T : Any> notEqualsCondition(
    name: String,
    value: T,
    ytType: TiType? = null,
    havingParams: MutableSet<YTQLParam<out Any>>? = null,
  ) = apply {
    when (value) {
      is Instant -> {
        require(ytType != null) { "For instant type yt type have to be not null" }
        (havingParams ?: whereParams).add(instantParam(name, YTQLParamOperator.NOT_EQUALS, value, ytType))
      }

      else -> (havingParams ?: whereParams).add(YTQLParam(name, YTQLParamOperator.NOT_EQUALS, value, ytType))
    }
  }

  /**
   * Adds a not-null-condition to the query.
   *
   * @param name the name of the column.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun notNullCondition(
    name: String,
    havingNotNullParams: MutableSet<YTQLParam<Unit>>? = null
  ) = apply { (havingNotNullParams ?: whereNotNullParams).add(YTQLParam(name, YTQLParamOperator.IS_NOT_NULL, Unit)) }

  /**
   * Adds an in-condition to the query.
   *
   * @param T the type of the value being compared.
   * @param name the name of the column.
   * @param value the collection of values to compare.
   * @param ytType the YT type information.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun <T : Any> inCondition(
    name: String,
    value: Iterable<T>,
    ytType: TiType? = null,
    havingListParams: MutableSet<YTQLListParam<out Any>>? = null,
  ) = apply {
    when {
      value.first() is Instant -> {
        require(ytType != null) { "For instant type yt type have to be not null" }
        (havingListParams ?: whereListParams).add(instantListParam(name, value.filterIsInstance<Instant>(), ytType))
      }

      else -> (havingListParams ?: whereListParams).add(YTQLListParam(name, value, ytType = ytType))
    }
  }

  /**
   * Adds an or-condition to the query.
   *
   * @param T the type of the value being compared.
   * @param firstNameAndValue the first column name and value pair.
   * @param secondNameAndValue the second column name and value pair.
   * @param firstYtType the YT type information for the first column.
   * @param secondYtType the YT type information for the second column.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun <T : Any> orCondition(
    firstNameAndValue: Pair<String, T>,
    secondNameAndValue: Pair<String, T>,
    firstYtType: TiType? = null,
    secondYtType: TiType? = null,
    havingOrParams: MutableSet<YTQLOrParam<out Any, out Any>>? = null,
  ) = apply {
    val firstName = firstNameAndValue.first
    val firstValue = firstNameAndValue.second
    val secondName = secondNameAndValue.first
    val secondValue = secondNameAndValue.second

    when {
      firstValue is Instant && secondValue is Instant -> {
        require(firstYtType != null && secondYtType != null) {
          "For instant types firstYtType and secondYtType have to be not null"
        }
        (havingOrParams ?: whereOrParams).add(
          YTQLOrParam(
            firstParam = instantParam(firstName, value = firstValue, ytType = firstYtType),
            secondParam = instantParam(secondName, value = secondValue, ytType = secondYtType)
          )
        )
      }

      else -> (havingOrParams ?: whereOrParams).add(
        YTQLOrParam(
          firstParam = YTQLParam(firstName, value = firstValue, ytType = firstYtType),
          secondParam = YTQLParam(secondName, value = secondValue, ytType = secondYtType)
        )
      )
    }
  }

  /**
   * Adds a less-condition to the query.
   *
   * @param T the type of the value being compared.
   * @param name the name of the column.
   * @param value the value to compare.
   * @param ytType the YT type information.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun <T : Any> lessCondition(
    name: String,
    value: T,
    ytType: TiType? = null,
    havingParams: MutableSet<YTQLParam<out Any>>? = null,
  ) = apply {
    when (value) {
      is Instant -> {
        require(ytType != null) { "For instant type yt type have to be not null" }
        (havingParams ?: whereParams).add(instantParam(name, YTQLParamOperator.LESS, value, ytType))
      }

      else -> (havingParams ?: whereParams).add(YTQLParam(name, YTQLParamOperator.LESS, value, ytType))
    }
  }

  /**
   * Adds a greater-condition to the query.
   *
   * @param T the type of the value being compared.
   * @param name the name of the column.
   * @param value the value to compare.
   * @param ytType the YT type information.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun <T : Any> greaterCondition(
    name: String,
    value: T,
    ytType: TiType? = null,
    havingParams: MutableSet<YTQLParam<out Any>>? = null,
  ) = apply {
    when (value) {
      is Instant -> {
        require(ytType != null) { "For instant type yt type have to be not null" }
        (havingParams ?: whereParams).add(instantParam(name, YTQLParamOperator.GREATER, value, ytType))
      }

      else -> (havingParams ?: whereParams).add(YTQLParam(name, YTQLParamOperator.GREATER, value, ytType))
    }
  }

  /**
   * Sets the limit for the number of rows to return.
   *
   * @param value the limit value.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun protectedLimit(value: Int) = apply { limit = value }

  /**
   * Builds the final `SelectRowsRequest` for the query.
   *
   * @param joinAction the join conditions to be applied.
   * @return the constructed `SelectRowsRequest`.
   */
  protected fun buildRequest(
    joinAction: (StringBuilder.() -> Unit)? = null,
    groupByAction: (StringBuilder.() -> Unit)? = null
  ) = SelectRowsRequest.of(buildString {
    append(selectStatement)
    joinAction?.invoke(this)
    if (hasParams(whereParams, whereNotNullParams, whereListParams, whereOrParams)) append(" WHERE ")
    appendCondition(whereParams, whereNotNullParams, whereListParams, whereOrParams)
    groupByAction?.invoke(this)
    orderByStatement?.let { append(orderByStatement) }
    limit?.let { append(" LIMIT $it") }
  })

  /**
   * Constructs the SELECT statement for the query.
   *
   * @param params the columns to select.
   * @param tableAlias the alias for the table.
   * @return the constructed SELECT statement.
   */
  protected fun selectFrom(
    params: String,
    tableAlias: String? = null
  ) = buildString {
    append("$params FROM [${mainTable.name}]")
    tableAlias?.let { append(" AS $it") }
  }

  /**
   * Builds the ORDER BY clause for the query.
   *
   * @param namesWithDirection the columns with their respective order directions.
   * @return the current `AbstractYTQLBuilder` instance.
   */
  protected fun buildOrderBy(namesWithDirection: Set<Pair<String, OrderDirection>>) = apply {
    orderByStatement = " ORDER BY ${namesWithDirection.joinToString { (name, direction) -> "$name $direction" }}"
  }

  protected fun hasParams(vararg params: Set<Any>) = params.any { it.isNotEmpty() }

  protected fun StringBuilder.appendCondition(
    params: MutableSet<YTQLParam<out Any>>,
    notNullParams: MutableSet<YTQLParam<Unit>>,
    listParams: MutableSet<YTQLListParam<out Any>>,
    orParams: MutableSet<YTQLOrParam<out Any, out Any>>,
  ) {
    params.forEachIndexed { index, param ->
      append("${param.name} ${operator(param)} ${value(param)}")
      appendAnd(index, params, notNullParams, listParams, orParams)
    }
    orParams.forEachIndexed { index, (firstParam, secondParam) ->
      append("(${firstParam.name} ${operator(firstParam)} ${value(firstParam)}")
      append(" OR ")
      append("${secondParam.name} ${operator(secondParam)} ${value(secondParam)})")
      appendAnd(index, orParams, notNullParams, listParams)
    }
    notNullParams.forEachIndexed { index, param ->
      append("${param.name} ${operator(param)}")
      appendAnd(index, notNullParams, listParams)
    }
    listParams.forEachIndexed { index, param ->
      append("${param.name} ${operator(param)} ${value(param)}")
      appendAnd(index, listParams)
    }
  }

  private fun StringBuilder.appendAnd(
    index: Int,
    current: Set<Any>,
    vararg next: Set<Any>?
  ) {
    if (index < current.size - 1 || next.any { it?.isNotEmpty() == true }) append(" AND ")
  }

  private fun operator(param: YTQLParam<out Any>) = param.operator.value

  private fun value(param: YTQLParam<out Any>) = when {

    param.value is Collection<*> -> {
      when {
        param.value.firstOrNull() is String || param.ytType?.isString == true -> param.value.joinToString(
          prefix = "(",
          postfix = ")",
          transform = { "\"$it\"" })

        else -> param.value.joinToString(prefix = "(", postfix = ")") { "$it" }
      }
    }

    param.value is String || param.ytType?.isString == true -> "\"${param.value}\""

    else -> param.value
  }

  private fun instantParam(
    name: String,
    operator: YTQLParamOperator = YTQLParamOperator.EQUALS,
    value: Instant,
    ytType: TiType,
  ): YTQLParam<Long> = when (ytType) {
    timestamp() -> YTQLParam(name, operator, value.toEpochMicro(), ytType)
    datetime() -> YTQLParam(name, operator, value.epochSecond, ytType)
    else -> throw IllegalArgumentException("Not allowed source type")
  }

  private fun instantListParam(
    name: String,
    value: List<Instant>,
    from: TiType,
  ): YTQLListParam<Any> = when (from) {
    timestamp() -> YTQLListParam(name, value.map { it.toEpochMicro() }, from)
    datetime() -> YTQLListParam(name, value.map { it.epochSecond }, from)
    else -> throw IllegalArgumentException("Not allowed source type")
  }

  /**
   * The `AbstractOrderByBuilder` interface provides a foundation for building ORDER BY clauses in queries.
   *
   * @param MT the type of the main table's entity.
   */
  interface AbstractOrderByBuilder<MT> {

    /**
     * Sets the limit for the number of rows to return.
     *
     * @param value the limit value.
     * @return the main `AbstractYTQLBuilder` instance.
     */
    fun limit(value: Int): AbstractYTQLBuilder<MT>
  }
}