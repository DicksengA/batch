package com.dickson.master.verticles

interface ProcessInstanceDao {
    fun upsertProcessInstance(processInstance: ProcessInstance)

}
