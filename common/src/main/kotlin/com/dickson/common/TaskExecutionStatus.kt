package com.dickson.common


enum class TaskExecutionStatus(val code: Int, val desc: String) {
    SUBMITTED_SUCCESS(0, "submit success"),
    RUNNING_EXECUTION(1, "running"),
    PAUSE(3, "pause"),
    FAILURE(6, "failure"),
    SUCCESS(7, "success"),
    NEED_FAULT_TOLERANCE(8, "need fault tolerance"),
    KILL(9, "kill"),
    DELAY_EXECUTION(12, "delay execution"),
    FORCED_SUCCESS(13, "forced success"),
    DISPATCH(17, "dispatch");

    fun isRunning(): Boolean {
        return this == RUNNING_EXECUTION
    }

    fun isSuccess(): Boolean {
        return this == SUCCESS
    }

    fun isForceSuccess(): Boolean {
        return this == FORCED_SUCCESS
    }

    fun isKill(): Boolean {
        return this == KILL
    }

    fun isFailure(): Boolean {
        return this == FAILURE || this == NEED_FAULT_TOLERANCE
    }

    fun isPause(): Boolean {
        return this == PAUSE
    }


    fun isFinished(): Boolean {
        return isSuccess() || isKill() || isFailure() || isPause()
    }

    fun isNeedFaultTolerance(): Boolean {
        return this == NEED_FAULT_TOLERANCE
    }


    fun shouldFailover(): Boolean {
        return (SUBMITTED_SUCCESS == this
                ) || (DISPATCH == this
                ) || (RUNNING_EXECUTION == this
                ) || (DELAY_EXECUTION == this)
    }

    override fun toString(): String {
        return "TaskExecutionStatus{code=$code, desc='$desc'}"
    }

    companion object {
        private val CODE_MAP: MutableMap<Int, TaskExecutionStatus> = HashMap()
        val needFailoverWorkflowInstanceState = intArrayOf(
                SUBMITTED_SUCCESS.code,
                DISPATCH.code,
                RUNNING_EXECUTION.code,
                DELAY_EXECUTION.code)

        init {
            for (executionStatus: TaskExecutionStatus in entries) {
                CODE_MAP[executionStatus.code] = executionStatus
            }
        }

        /**
         * Get `TaskExecutionStatus` by code, if the code is invalidated will throw [IllegalArgumentException].
         */
        fun of(code: Int): TaskExecutionStatus {
            val taskExecutionStatus = CODE_MAP.get(code)
                    ?: throw IllegalArgumentException(String.format("The task execution status code: %s is invalidated",
                            code))
            return taskExecutionStatus
        }
    }
}
