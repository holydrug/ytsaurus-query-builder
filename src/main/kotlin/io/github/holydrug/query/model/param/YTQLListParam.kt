package io.github.holydrug.query.model.param

import tech.ytsaurus.typeinfo.TiType

class YTQLListParam<T>(
    name: String,
    value: Collection<T>,
    ytType: TiType? = null
) : YTQLParam<Collection<T>>(name, YTQLParamOperator.IN, value, ytType)