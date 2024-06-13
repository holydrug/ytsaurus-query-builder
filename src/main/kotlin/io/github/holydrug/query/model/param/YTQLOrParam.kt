package io.github.holydrug.query.model.param

data class YTQLOrParam<LHS, RHS>(
    val firstParam: YTQLParam<LHS>,
    val secondParam: YTQLParam<RHS>
)