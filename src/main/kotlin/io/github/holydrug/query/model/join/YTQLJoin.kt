package io.github.holydrug.query.model.join

import io.github.holydrug.scheme.YtCrudTable

data class YTQLJoin<T>(
    val type: YTQLJoinType = YTQLJoinType.INNER,
    val tableToJoin: YtCrudTable<T>,
    val condition: YTQLJoinCondition
)