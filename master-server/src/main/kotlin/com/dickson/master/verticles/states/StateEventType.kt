package com.dickson.master.verticles.states


enum class StateEventType(val code: Int, val descp: String) {
    PROCESS_STATE_CHANGE(0, "process state change"),
    TASK_STATE_CHANGE(1, "task state change"),
    PROCESS_TIMEOUT(2, "process timeout"),
    TASK_TIMEOUT(3, "task timeout"),
    WAIT_TASK_GROUP(4, "wait task group"),
    TASK_RETRY(5, "task retry"),
    PROCESS_BLOCKED(6, "process blocked")

}
