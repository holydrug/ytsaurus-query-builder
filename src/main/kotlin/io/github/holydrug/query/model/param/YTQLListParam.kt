package io.github.holydrug.query.model.param

import tech.ytsaurus.typeinfo.TiType

class YTQLListParam<T>(
  name: String,
  value: Iterable<T>,
  ytType: TiType? = null
) : YTQLParam<Iterable<T>>(name, YTQLParamOperator.IN, value, ytType)