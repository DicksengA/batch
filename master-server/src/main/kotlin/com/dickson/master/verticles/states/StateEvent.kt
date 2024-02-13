package com.dickson.master.verticles.states


interface StateEvent {
    fun getProcessInstanceId(): Long
    fun getTaskInstanceId(): Int

    val taskInstanceId: Int

    val type: StateEventType
    val key: String
    val context: String
}
