package io.github.holydrug.query.model.param

import tech.ytsaurus.typeinfo.TiType

open class YTQLParam<T>(
    val name: String,
    val operator: YTQLParamOperator = YTQLParamOperator.EQUALS,
    val value: T,
    val ytType: TiType? = null
)