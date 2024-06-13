package io.github.holydrug.scheme

import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.ysontree.YTreeStringNode

interface YtSchema {

    val parent: YPath
    val schemaVersion: YTreeStringNode

    fun listTables(): List<AbstractYtTable<*>> = listOf()
    fun listQueues(): List<AbstractYtTable<*>> = listOf()
    fun listConsumers(): List<AbstractYtTable<*>> = listOf()
}