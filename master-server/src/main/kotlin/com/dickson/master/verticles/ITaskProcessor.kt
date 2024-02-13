package com.dickson.master.verticles

interface ITaskProcessor {

    fun init(taskInstance: TaskInstance?, processInstance: ProcessInstance?)

    fun action(taskAction: TaskAction?): Boolean

    fun getType(): String?

    fun taskInstance(): TaskInstance?

}
