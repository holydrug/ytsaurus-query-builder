package io.github.holydrug.query.model.selector

data class YtSelector(val value: String) {
  companion object {

    fun column(vararg name: String) = YtSelector(name.joinToString { it })

    fun count(alias: String) = YtSelector("SUM(1) AS $alias")

    fun sum(vararg columnNameWIthAlias: Pair<String, String>) = YtSelector(
      columnNameWIthAlias.joinToString { (columnName, alias) -> "SUM($columnName) AS $alias" }
    )

    fun min(vararg columnNameWIthAlias: Pair<String, String>) = YtSelector(
      columnNameWIthAlias.joinToString { (columnName, alias) -> "MIN($columnName) AS $alias" }
    )

    fun max(vararg columnNameWIthAlias: Pair<String, String>) = YtSelector(
      columnNameWIthAlias.joinToString { (columnName, alias) -> "MAX($columnName) AS $alias" }
    )

    fun avg(vararg columnNameWIthAlias: Pair<String, String>) = YtSelector(
      columnNameWIthAlias.joinToString { (columnName, alias) -> "AVG($columnName) AS $alias" }
    )
  }
}