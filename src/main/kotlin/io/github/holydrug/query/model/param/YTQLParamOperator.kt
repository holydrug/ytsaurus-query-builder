package io.github.holydrug.query.model.param

enum class YTQLParamOperator(val value: String) {
  LESS("<"),
  EQUAL_OR_LESS("<="),
  GREATER(">"),
  EQUAL_OR_GREATER(">="),
  EQUALS("="),
  NOT_EQUALS("!="),
  IN("IN"),
  IS_NOT_NULL("IS NOT NULL"),
}