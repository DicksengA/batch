package com.dickson.master.verticles

interface ProcessService {

    fun loadTaskGroupQueue(taskId: Int): TaskGroupQueue
    fun updateTaskGroupQueueStatus(taskId: Long, code: Int)
    fun robTaskGroupResource(taskGroupQueue: TaskGroupQueue): Boolean

}
