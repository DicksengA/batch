package com.dickson.master.verticles


import com.dickson.common.Constants
import com.dickson.common.DAG
import com.dickson.common.enums.Flag
import com.dickson.common.enums.TaskGroupQueueStatus
import com.dickson.master.verticles.dag.DagHelper
import com.dickson.master.verticles.states.StateEvent
import io.smallrye.mutiny.coroutines.asFlow
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor
import io.vertx.core.impl.ContextInternal
import io.vertx.mutiny.core.Context
import io.vertx.mutiny.core.Vertx
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory

import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors

/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/



/**
 * Workflow execute task, used to execute a workflow instance.
 */
class WorkflowExecuteRunnable(
        val vertx: Vertx,
        val context: Context = vertx.orCreateContext,
        processInstance: ProcessInstance,
        commandService: CommandService,
        processService: ProcessService,
        processInstanceDao: ProcessInstanceDao,
        nettyExecutorManager: NettyExecutorManager,
        processAlertManager: ProcessAlertManager,
        masterConfig: MasterConfig,
        stateWheelExecuteThread: StateWheelExecuteThread,
        curingParamsService: CuringParamsService,
        taskInstanceDao: TaskInstanceDao,
        taskDefinitionLogDao: TaskDefinitionLogDao) : Callable<WorkflowSubmitStatue> {
    private val processService: ProcessService
    private val commandService: CommandService
    private val processInstanceDao: ProcessInstanceDao
    private val taskInstanceDao: TaskInstanceDao
    private val taskDefinitionLogDao: TaskDefinitionLogDao
    private val processAlertManager: ProcessAlertManager
    private val nettyExecutorManager: NettyExecutorManager
    var processInstance: ProcessInstance
    private lateinit var processDefinition: ProcessDefinition
    private var dag: DAG<String, TaskNode, TaskNodeRelation>? = null

    var key: String = ""

    /**
     * unique key of workflow
     */
    fun getKey(): String {
        if (processDefinition == null || key.isNotEmpty()) {
            return key
        }

//        key = java.lang.String.format("%d_%d_%d",
//                processDefinition.getCode(),
//                processDefinition?.getVersion(),
//                processInstance.getId())
        return key
    }


    private var workflowRunnableStatus = WorkflowRunnableStatus.CREATED

    /**
     * submit failure nodes
     */
    private var taskFailedSubmit = false

    /**
     * task instance hash map, taskId as key
     */
    private val taskInstanceMap: MutableMap<Int?, TaskInstance> = ConcurrentHashMap<Int?, TaskInstance>()

    /**
     * running taskProcessor, taskCode as key, taskProcessor as value
     * only on taskProcessor per taskCode
     */
    private val activeTaskProcessorMaps: MutableMap<Long, ITaskProcessor> = ConcurrentHashMap<Long, ITaskProcessor>()

    /**
     * valid task map, taskCode as key, taskId as value
     * in a DAG, only one taskInstance per taskCode is valid
     */
    private val validTaskMap: MutableMap<Long, Int> = ConcurrentHashMap()

    /**
     * error task map, taskCode as key, taskInstanceId as value
     * in a DAG, only one taskInstance per taskCode is valid
     */
    private val errorTaskMap: MutableMap<Long, Int> = ConcurrentHashMap()

    /**
     * complete task map, taskCode as key, taskInstanceId as value
     * in a DAG, only one taskInstance per taskCode is valid
     */
    private val completeTaskMap: MutableMap<Long, Int> = ConcurrentHashMap()

    /**
     * depend failed task set
     */
    private val dependFailedTaskSet: MutableSet<Long> = Collections.newSetFromMap(ConcurrentHashMap())

    /**
     * forbidden task map, code as key
     */
    private val forbiddenTaskMap: MutableMap<Long, TaskNode> = ConcurrentHashMap<Long, TaskNode>()

    /**
     * skip task map, code as key
     */
    private val skipTaskNodeMap: Map<String, TaskNode> = ConcurrentHashMap<String, TaskNode>()

    /**
     * complement date list
     */
    private var complementListDate: List<Date> = LinkedList()

    /**
     * state event queue
     */
    private val stateEvents: Queue<StateEvent> = ConcurrentLinkedQueue<StateEvent>()

    /**
     * The StandBy task list, will be executed, need to know, the taskInstance in this queue may doesn't have id.
     */
    private val readyToSubmitTaskQueue: PeerTaskInstancePriorityQueue = PeerTaskInstancePriorityQueue()

    /**
     * wait to retry taskInstance map, taskCode as key, taskInstance as value
     * before retry, the taskInstance id is 0
     */
    private val waitToRetryTaskInstanceMap: MutableMap<Long, TaskInstance> = ConcurrentHashMap<Long, TaskInstance>()
    private val stateWheelExecuteThread: StateWheelExecuteThread
    private val curingParamsService: CuringParamsService
    private val masterAddress: String

    /**
     * @param processInstance         processInstance
     * @param processService          processService
     * @param processInstanceDao      processInstanceDao
     * @param nettyExecutorManager    nettyExecutorManager
     * @param processAlertManager     processAlertManager
     * @param masterConfig            masterConfig
     * @param stateWheelExecuteThread stateWheelExecuteThread
     */
    init {
        this.processService = processService
        this.commandService = commandService
        this.processInstanceDao = processInstanceDao
        this.processInstance = processInstance
        this.nettyExecutorManager = nettyExecutorManager
        this.processAlertManager = processAlertManager
        this.stateWheelExecuteThread = stateWheelExecuteThread
        this.curingParamsService = curingParamsService
        this.taskInstanceDao = taskInstanceDao
        this.taskDefinitionLogDao = taskDefinitionLogDao
        masterAddress = masterConfig.getListenPort()
        //TaskMetrics.registerTaskPrepared(readyToSubmitTaskQueue::size)
    }

    /**
     * the process start nodes are submitted completely.
     */
    fun isStart(): Boolean {

        return WorkflowRunnableStatus.STARTED == workflowRunnableStatus
    }


    val eventQueue = UnicastProcessor.create(stateEvents, null)

    fun handleEventss2() {
        val asCoroutineDispatcher = (context.delegate as ContextInternal).nettyEventLoop().asCoroutineDispatcher()
        val job = CoroutineScope(asCoroutineDispatcher).launch {
            eventQueue.asFlow().collect {
                handleEvents(it)
            }
        }

    }

    /**
     * handle event
     */
    suspend fun handleEvents(stateEvent: StateEvent) {
        try {

//                LoggerUtils.setWorkflowAndTaskInstanceIDMDC(stateEvent.getProcessInstanceId(),
//                        stateEvent.getTaskInstanceId())
            // if state handle success then will remove this state, otherwise will retry this state next time.
            // The state should always handle success except database error.
            checkProcessInstance(stateEvent)

            val stateEventHandler: StateEventHandler = StateEventHandlerManager.getStateEventHandler(stateEvent.type)
                    .orElseThrow {
                        StateEventHandleError(
                                "Cannot find handler for the given state event")
                    }
            logger.info("Begin to handle state event, {}", stateEvent)
            if (stateEventHandler.handleStateEvent(this, stateEvent)) {
                stateEvents.remove(stateEvent)
            }
        } catch (stateEventHandleError: StateEventHandleError) {
            logger.error("State event handle error, will remove this event: {}", stateEvent, stateEventHandleError)
            stateEvents.remove(stateEvent)
            delay(Constants.SLEEP_TIME_MILLIS)
        } catch (stateEventHandleException: StateEventHandleException) {
            logger.error("State event handle error, will retry this event: {}",
                    stateEvent,
                    stateEventHandleException)
            delay(Constants.SLEEP_TIME_MILLIS)
        } catch (e: Exception) {
            // we catch the exception here, since if the state event handle failed, the state event will still keep
            // in the stateEvents queue.
            logger.error("State event handle error, get a unknown exception, will retry this event: {}",
                    stateEvent,
                    e)
            delay(Constants.SLEEP_TIME_MILLIS)
        } finally {
            //LoggerUtils.removeWorkflowAndTaskInstanceIdMDC()
        }
    }


    fun addStateEvent(stateEvent: StateEvent): Boolean {
        if (processInstance.getId() !== stateEvent.getProcessInstanceId()) {
            logger.info("state event would be abounded :{}", stateEvent)
            return false
        }
        stateEvents.add(stateEvent)
        return true
    }

    fun eventSize(): Int {
        return stateEvents.size
    }

    fun getProcessInstance(): ProcessInstance {
        return processInstance
    }

    fun checkForceStartAndWakeUp(stateEvent: StateEvent): Boolean {
        val taskGroupQueue: TaskGroupQueue = processService.loadTaskGroupQueue(stateEvent.taskInstanceId)
        if (taskGroupQueue.getForceStart() === Flag.YES.getCode()) {
            val taskInstance: TaskInstance = taskInstanceDao.findTaskInstanceById(stateEvent.taskInstanceId)
            val taskProcessor: ITaskProcessor? = activeTaskProcessorMaps[taskInstance.getTaskCode()]
            if (taskProcessor != null) {
                taskProcessor.action(TaskAction.DISPATCH)
                processService.updateTaskGroupQueueStatus(taskGroupQueue.getTaskId(),
                        TaskGroupQueueStatus.ACQUIRE_SUCCESS.getCode())
                return true
            } else {
                logger.warn("Unable to find task processor for ${taskInstance.getTaskCode()}")
            }
        }
        if (taskGroupQueue.getInQueue() === Flag.YES.getCode()) {
            val acquireTaskGroup: Boolean = processService.robTaskGroupResource(taskGroupQueue)
            if (acquireTaskGroup) {
                val taskInstance: TaskInstance = taskInstanceDao.findTaskInstanceById(stateEvent.getTaskInstanceId())
                val taskProcessor: ITaskProcessor? = activeTaskProcessorMaps[taskInstance.getTaskCode()]
                taskProcessor?.action(TaskAction.DISPATCH)
                return true
            }
        }
        return false
    }

//    fun processTimeout() {
//        val projectUser: ProjectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId())
//        processAlertManager.sendProcessTimeoutAlert(processInstance, projectUser)
//    }
//
//    fun taskTimeout(taskInstance: TaskInstance?) {
//        val projectUser: ProjectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId())
//        processAlertManager.sendTaskTimeoutAlert(processInstance, taskInstance, projectUser)
//    }

    @Throws(StateEventHandleException::class)
    fun taskFinished(taskInstance: TaskInstance) {
        logger.info("TaskInstance finished task code:{} state:{}", taskInstance.getTaskCode(), taskInstance.getState())
        try {
            activeTaskProcessorMaps.remove(taskInstance.getTaskCode())
            stateWheelExecuteThread.removeTask4TimeoutCheck(processInstance, taskInstance)
            stateWheelExecuteThread.removeTask4RetryCheck(processInstance, taskInstance)
            stateWheelExecuteThread.removeTask4StateCheck(processInstance, taskInstance)
            if (taskInstance.getState().isSuccess()) {
                completeTaskMap[taskInstance.getTaskCode()] = taskInstance.getId()
                // todo: merge the last taskInstance
                processInstance.setVarPool(taskInstance.getVarPool())
                processInstanceDao.upsertProcessInstance(processInstance)
                if (!processInstance.isBlocked()) {
                    submitPostNode(taskInstance.getTaskCode().toString())
                }
            } else if (taskInstance.taskCanRetry() && !processInstance.getState().isReadyStop()) {
                // retry task
                logger.info("Retry taskInstance taskInstance state: {}", taskInstance.getState())
                retryTaskInstance(taskInstance)
            } else if (taskInstance.getState().isFailure()) {
                completeTaskMap[taskInstance.getTaskCode()] = taskInstance.getId()
                errorTaskMap[taskInstance.getTaskCode()] = taskInstance.getId()
                // There are child nodes and the failure policy is: CONTINUE
                if (processInstance.getFailureStrategy() === FailureStrategy.CONTINUE && DagHelper.haveAllNodeAfterNode(taskInstance.getTaskCode().toString(),
                                dag)) {
                    submitPostNode(taskInstance.getTaskCode().toString())
                } else {
                    if (processInstance.getFailureStrategy() === FailureStrategy.END) {
                        killAllTasks()
                    }
                }
            } else if (taskInstance.getState().isFinished()) {
                // todo: when the task instance type is pause, then it should not in completeTaskMap
                completeTaskMap[taskInstance.getTaskCode()] = taskInstance.getId()
            }
            logger.info("TaskInstance finished will try to update the workflow instance state, task code:{} state:{}",
                    taskInstance.getTaskCode(),
                    taskInstance.getState())
            this.updateProcessInstanceState()
        } catch (ex: Exception) {
            logger.error("Task finish failed, get a exception, will remove this taskInstance from completeTaskMap", ex)
            // remove the task from complete map, so that we can finish in the next time.
            completeTaskMap.remove(taskInstance.getTaskCode())
            throw ex
        }
    }

    /**
     * release task group
     *
     */
    fun releaseTaskGroup(taskInstance: TaskInstance) {
        logger.info("Release task group")
        if (taskInstance.getTaskGroupId() > 0) {
            val nextTaskInstance: TaskInstance = processService.releaseTaskGroup(taskInstance)
            if (nextTaskInstance != null) {
                if (nextTaskInstance.getProcessInstanceId() === taskInstance.getProcessInstanceId()) {
                    val nextEvent: TaskStateEvent = TaskStateEvent.builder()
                            .processInstanceId(processInstance.getId())
                            .taskInstanceId(nextTaskInstance.getId())
                            .type(StateEventType.WAIT_TASK_GROUP)
                            .build()
                    stateEvents.add(nextEvent)
                } else {
                    val processInstance: ProcessInstance = processService.findProcessInstanceById(nextTaskInstance.getProcessInstanceId())
                    processService.sendStartTask2Master(processInstance, nextTaskInstance.getId(),
                            org.apache.dolphinscheduler.remote.command.CommandType.TASK_WAKEUP_EVENT_REQUEST)
                }
            }
        }
    }

    /**
     * crate new task instance to retry, different objects from the original
     *
     */
    @Throws(StateEventHandleException::class)
    private fun retryTaskInstance(taskInstance: TaskInstance) {
        if (!taskInstance.taskCanRetry()) {
            return
        }
        val newTaskInstance: TaskInstance? = cloneRetryTaskInstance(taskInstance)
        if (newTaskInstance == null) {
            logger.error("Retry task fail because new taskInstance is null, task code:{}, task id:{}",
                    taskInstance.getTaskCode(),
                    taskInstance.getId())
            return
        }
        waitToRetryTaskInstanceMap[newTaskInstance.getTaskCode()] = newTaskInstance
        if (!taskInstance.retryTaskIntervalOverTime()) {
            logger.info(
                    "Failure task will be submitted, process id: {}, task instance code: {}, state: {}, retry times: {} / {}, interval: {}",
                    processInstance.getId(), newTaskInstance.getTaskCode(),
                    newTaskInstance.getState(), newTaskInstance.getRetryTimes(), newTaskInstance.getMaxRetryTimes(),
                    newTaskInstance.getRetryInterval())
            stateWheelExecuteThread.addTask4TimeoutCheck(processInstance, newTaskInstance)
            stateWheelExecuteThread.addTask4RetryCheck(processInstance, newTaskInstance)
        } else {
            addTaskToStandByList(newTaskInstance)
            submitStandByTask()
            waitToRetryTaskInstanceMap.remove(newTaskInstance.getTaskCode())
        }
    }

    /**
     * update process instance
     */
    fun refreshProcessInstance(processInstanceId: Int) {
        logger.info("process instance update: {}", processInstanceId)
        val newProcessInstance: ProcessInstance = processService.findProcessInstanceById(processInstanceId)
        // just update the processInstance field(this is soft copy)
        BeanUtils.copyProperties(newProcessInstance, processInstance)
        processDefinition = processService.findProcessDefinition(processInstance.getProcessDefinitionCode(),
                processInstance.getProcessDefinitionVersion())
        processInstance.setProcessDefinition(processDefinition)
    }

    /**
     * update task instance
     */
    fun refreshTaskInstance(taskInstanceId: Int) {
        logger.info("task instance update: {} ", taskInstanceId)
        val taskInstance: TaskInstance = taskInstanceDao.findTaskInstanceById(taskInstanceId)
        if (taskInstance == null) {
            logger.error("can not find task instance, id:{}", taskInstanceId)
            return
        }
        processService.packageTaskInstance(taskInstance, processInstance)
        taskInstanceMap[taskInstance.getId()] = taskInstance
        validTaskMap.remove(taskInstance.getTaskCode())
        if (Flag.YES === taskInstance.getFlag()) {
            validTaskMap[taskInstance.getTaskCode()] = taskInstance.getId()
        }
    }

    /**
     * check process instance by state event
     */
    @Throws(StateEventHandleError::class)
    fun checkProcessInstance(stateEvent: StateEvent) {
        if (processInstance.getId() !== stateEvent.getProcessInstanceId()) {
            throw StateEventHandleError("The event doesn't contains process instance id")
        }
    }

    /**
     * check if task instance exist by state event
     */
    @Throws(StateEventHandleError::class)
    fun checkTaskInstanceByStateEvent(stateEvent: TaskStateEvent) {
        if (stateEvent.getTaskInstanceId() === 0) {
            throw StateEventHandleError("The taskInstanceId is 0")
        }
        if (!taskInstanceMap.containsKey(stateEvent.getTaskInstanceId())) {
            throw StateEventHandleError("Cannot find the taskInstance from taskInstanceMap")
        }
    }

    /**
     * check if task instance exist by id
     */
    fun checkTaskInstanceById(taskInstanceId: Int): Boolean {
        return if (taskInstanceMap.isEmpty()) {
            false
        } else taskInstanceMap.containsKey(taskInstanceId)
    }

    /**
     * get task instance from memory
     */
    fun getTaskInstance(taskInstanceId: Int): Optional<TaskInstance> {
        return if (taskInstanceMap.containsKey(taskInstanceId)) {
            Optional.ofNullable<TaskInstance>(taskInstanceMap[taskInstanceId])
        } else Optional.empty<TaskInstance>()
    }

    fun getTaskInstance(taskCode: Long): Optional<TaskInstance> {
        if (taskInstanceMap.isEmpty()) {
            return Optional.empty<TaskInstance>()
        }
        for (taskInstance in taskInstanceMap.values) {
            if (taskInstance.getTaskCode() === taskCode) {
                return Optional.of<TaskInstance>(taskInstance)
            }
        }
        return Optional.empty<TaskInstance>()
    }

    fun getActiveTaskInstanceByTaskCode(taskCode: Long): Optional<TaskInstance> {
        val taskInstanceId = validTaskMap[taskCode]
        return if (taskInstanceId != null) {
            Optional.ofNullable<TaskInstance>(taskInstanceMap[taskInstanceId])
        } else Optional.empty<TaskInstance>()
    }

    fun getRetryTaskInstanceByTaskCode(taskCode: Long): Optional<TaskInstance> {
        return if (waitToRetryTaskInstanceMap.containsKey(taskCode)) {
            Optional.ofNullable<TaskInstance>(waitToRetryTaskInstanceMap[taskCode])
        } else Optional.empty<TaskInstance>()
    }

    fun processBlock() {
        val projectUser: ProjectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId())
        processAlertManager.sendProcessBlockingAlert(processInstance, projectUser)
        logger.info("processInstance {} block alert send successful!", processInstance.getId())
    }

    fun processComplementData(): Boolean {
        if (!needComplementProcess()) {
            return false
        }

        // when the serial complement is executed, the next complement instance is created,
        // and this method does not need to be executed when the parallel complement is used.
        if (processInstance.getState().isReadyStop() || !processInstance.getState().isFinished()) {
            return false
        }
        var scheduleDate: Date = processInstance.getScheduleTime()
        if (scheduleDate == null) {
            scheduleDate = complementListDate[0]
        } else if (processInstance.getState().isFinished()) {
            endProcess()
            if (complementListDate.isEmpty()) {
                logger.info("process complement end. process id:{}", processInstance.getId())
                return true
            }
            val index = complementListDate.indexOf(scheduleDate)
            if (index >= complementListDate.size - 1 || !processInstance.getState().isSuccess()) {
                logger.info("process complement end. process id:{}", processInstance.getId())
                // complement data ends || no success
                return true
            }
            logger.info("process complement continue. process id:{}, schedule time:{} complementListDate:{}",
                    processInstance.getId(), processInstance.getScheduleTime(), complementListDate)
            scheduleDate = complementListDate[index + 1]
        }
        // the next process complement
        val create = createComplementDataCommand(scheduleDate)
        if (create > 0) {
            logger.info("create complement data command successfully.")
        }
        return true
    }

    private fun createComplementDataCommand(scheduleDate: Date?): Int {
        val command = Command()
        command.setScheduleTime(scheduleDate)
        command.setCommandType(CommandType.COMPLEMENT_DATA)
        command.setProcessDefinitionCode(processInstance.getProcessDefinitionCode())
        val cmdParam: MutableMap<String, String> = JSONUtils.toMap(processInstance.getCommandParam())
        if (cmdParam.containsKey(CMD_PARAM_RECOVERY_START_NODE_STRING)) {
            cmdParam.remove(CMD_PARAM_RECOVERY_START_NODE_STRING)
        }
        if (cmdParam.containsKey(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST)) {
            cmdParam.replace(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST,
                    cmdParam[CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST]
                            .substring(cmdParam[CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST].indexOf(COMMA) + 1))
        }
        if (cmdParam.containsKey(CMD_PARAM_COMPLEMENT_DATA_START_DATE)) {
            cmdParam.replace(CMD_PARAM_COMPLEMENT_DATA_START_DATE,
                    DateUtils.format(scheduleDate, YYYY_MM_DD_HH_MM_SS, null))
        }
        command.setCommandParam(JSONUtils.toJsonString(cmdParam))
        command.setTaskDependType(processInstance.getTaskDependType())
        command.setFailureStrategy(processInstance.getFailureStrategy())
        command.setWarningType(processInstance.getWarningType())
        command.setWarningGroupId(processInstance.getWarningGroupId())
        command.setStartTime(Date())
        command.setExecutorId(processInstance.getExecutorId())
        command.setUpdateTime(Date())
        command.setProcessInstancePriority(processInstance.getProcessInstancePriority())
        command.setWorkerGroup(processInstance.getWorkerGroup())
        command.setEnvironmentCode(processInstance.getEnvironmentCode())
        command.setDryRun(processInstance.getDryRun())
        command.setProcessInstanceId(0)
        command.setProcessDefinitionVersion(processInstance.getProcessDefinitionVersion())
        command.setTestFlag(processInstance.getTestFlag())
        return commandService.createCommand(command)
    }

    private fun needComplementProcess(): Boolean {
        return processInstance.isComplementData() && Flag.NO === processInstance.getIsSubProcess()
    }

    /**
     * ProcessInstance start entrypoint.
     */
    override fun call(): WorkflowSubmitStatue {
        if (isStart()) {
            // This case should not been happened
            logger.warn("[WorkflowInstance-{}] The workflow has already been started", processInstance.getId())
            return WorkflowSubmitStatue.DUPLICATED_SUBMITTED
        }
        return try {
            LoggerUtils.setWorkflowInstanceIdMDC(processInstance.getId())
            if (workflowRunnableStatus == org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable.WorkflowRunnableStatus.CREATED) {
                buildFlowDag()
                workflowRunnableStatus = org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable.WorkflowRunnableStatus.INITIALIZE_DAG
                logger.info("workflowStatue changed to :{}", workflowRunnableStatus)
            }
            if (workflowRunnableStatus == org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable.WorkflowRunnableStatus.INITIALIZE_DAG) {
                initTaskQueue()
                workflowRunnableStatus = org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable.WorkflowRunnableStatus.INITIALIZE_QUEUE
                logger.info("workflowStatue changed to :{}", workflowRunnableStatus)
            }
            if (workflowRunnableStatus == org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable.WorkflowRunnableStatus.INITIALIZE_QUEUE) {
                submitPostNode(null)
                workflowRunnableStatus = org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable.WorkflowRunnableStatus.STARTED
                logger.info("workflowStatue changed to :{}", workflowRunnableStatus)
            }
            WorkflowSubmitStatue.SUCCESS
        } catch (e: Exception) {
            logger.error("Start workflow error", e)
            WorkflowSubmitStatue.FAILED
        } finally {
            LoggerUtils.removeWorkflowInstanceIdMDC()
        }
    }

    /**
     * process end handle
     */
    fun endProcess() {
        stateEvents.clear()
        if (processDefinition.getExecutionType().typeIsSerialWait() || processDefinition.getExecutionType()
                        .typeIsSerialPriority()) {
            checkSerialProcess(processDefinition)
        }
        val projectUser: ProjectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId())
        processAlertManager.sendAlertProcessInstance(processInstance, validTaskList, projectUser)
        if (processInstance.getState().isSuccess()) {
            processAlertManager.closeAlert(processInstance)
        }
        if (checkTaskQueue()) {
            // release task group
            processService.releaseAllTaskGroup(processInstance.getId())
        }
    }

    fun checkSerialProcess(processDefinition: ProcessDefinition?) {
        var nextInstanceId: Int = processInstance.getNextProcessInstanceId()
        if (nextInstanceId == 0) {
            val nextProcessInstance: ProcessInstance = processService.loadNextProcess4Serial(processInstance.getProcessDefinition().getCode(),
                    WorkflowExecutionStatus.SERIAL_WAIT.getCode(), processInstance.getId()) ?: return
            val nextReadyStopProcessInstance: ProcessInstance = processService.loadNextProcess4Serial(processInstance.getProcessDefinition().getCode(),
                    WorkflowExecutionStatus.READY_STOP.getCode(), processInstance.getId())
            if (processDefinition.getExecutionType().typeIsSerialPriority() && nextReadyStopProcessInstance != null) {
                return
            }
            nextInstanceId = nextProcessInstance.getId()
        }
        val nextProcessInstance: ProcessInstance = processService.findProcessInstanceById(nextInstanceId)
        if (nextProcessInstance.getState().isFinished() || nextProcessInstance.getState().isRunning()) {
            return
        }
        val cmdParam: MutableMap<String, Any> = HashMap()
        cmdParam[CMD_PARAM_RECOVER_PROCESS_ID_STRING] = nextInstanceId
        val command = Command()
        command.setCommandType(CommandType.RECOVER_SERIAL_WAIT)
        command.setProcessInstanceId(nextProcessInstance.getId())
        command.setProcessDefinitionCode(processDefinition.getCode())
        command.setProcessDefinitionVersion(processDefinition.getVersion())
        command.setCommandParam(JSONUtils.toJsonString(cmdParam))
        commandService.createCommand(command)
    }

    /**
     * Generate process dag
     *
     * @throws Exception exception
     */
    @Throws(Exception::class)
    private fun buildFlowDag() {
        processDefinition = processService.findProcessDefinition(processInstance.getProcessDefinitionCode(),
                processInstance.getProcessDefinitionVersion())
        processInstance.setProcessDefinition(processDefinition)
        val recoverNodeList: List<TaskInstance> = getRecoverTaskInstanceList(processInstance.getCommandParam())
        val processTaskRelations: List<ProcessTaskRelation> = processService.findRelationByCode(processDefinition.getCode(), processDefinition.getVersion())
        val taskDefinitionLogs: List<TaskDefinitionLog> = taskDefinitionLogDao.getTaskDefineLogListByRelation(processTaskRelations)
        val taskNodeList: List<TaskNode?> = processService.transformTask(processTaskRelations, taskDefinitionLogs)
        forbiddenTaskMap.clear()
        taskNodeList.forEach(Consumer<TaskNode> { taskNode: TaskNode ->
            if (taskNode.isForbidden()) {
                forbiddenTaskMap[taskNode.getCode()] = taskNode
            }
        })

        // generate process to get DAG info
        val recoveryNodeCodeList = getRecoveryNodeCodeList(recoverNodeList)
        val startNodeNameList = parseStartNodeName(processInstance.getCommandParam())
        val processDag: ProcessDag = generateFlowDag(taskNodeList, startNodeNameList, recoveryNodeCodeList,
                processInstance.getTaskDependType())
        if (processDag == null) {
            logger.error("ProcessDag is null")
            return
        }
        // generate process dag
        dag = DagHelper.buildDagGraph(processDag)
        logger.info("Build dag success, dag: {}", dag)
    }

    /**
     * init task queue
     */
    @Throws(StateEventHandleException::class, CronParseException::class)
    private fun initTaskQueue() {
        taskFailedSubmit = false
        activeTaskProcessorMaps.clear()
        dependFailedTaskSet.clear()
        completeTaskMap.clear()
        errorTaskMap.clear()
        if (!isNewProcessInstance) {
            logger.info("The workflowInstance is not a newly running instance, runtimes: {}, recover flag: {}",
                    processInstance.getRunTimes(),
                    processInstance.getRecovery())
            val validTaskInstanceList: List<TaskInstance> = taskInstanceDao.findValidTaskListByProcessId(processInstance.getId(),
                    processInstance.getTestFlag())
            for (task in validTaskInstanceList) {
                try {
                    LoggerUtils.setWorkflowAndTaskInstanceIDMDC(task.getProcessInstanceId(), task.getId())
                    logger.info(
                            "Check the taskInstance from a exist workflowInstance, existTaskInstanceCode: {}, taskInstanceStatus: {}",
                            task.getTaskCode(),
                            task.getState())
                    if (validTaskMap.containsKey(task.getTaskCode())) {
                        logger.warn(
                                "Have same taskCode taskInstance when init task queue, need to check taskExecutionStatus, taskCode:{}",
                                task.getTaskCode())
                        val oldTaskInstanceId = validTaskMap[task.getTaskCode()]!!
                        val oldTaskInstance: TaskInstance? = taskInstanceMap[oldTaskInstanceId]
                        if (!oldTaskInstance.getState().isFinished() && task.getState().isFinished()) {
                            task.setFlag(Flag.NO)
                            taskInstanceDao.updateTaskInstance(task)
                            continue
                        }
                    }
                    validTaskMap[task.getTaskCode()] = task.getId()
                    taskInstanceMap[task.getId()] = task
                    if (task.isTaskComplete()) {
                        logger.info("TaskInstance is already complete.")
                        completeTaskMap[task.getTaskCode()] = task.getId()
                        continue
                    }
                    if (task.isConditionsTask() || DagHelper.haveConditionsAfterNode(task.getTaskCode().toString(),
                                    dag)) {
                        continue
                    }
                    if (task.taskCanRetry()) {
                        if (task.getState().isNeedFaultTolerance()) {
                            logger.info("TaskInstance needs fault tolerance, will be added to standby list.")
                            task.setFlag(Flag.NO)
                            taskInstanceDao.updateTaskInstance(task)

                            // tolerantTaskInstance add to standby list directly
                            val tolerantTaskInstance: TaskInstance? = cloneTolerantTaskInstance(task)
                            addTaskToStandByList(tolerantTaskInstance)
                        } else {
                            logger.info("Retry taskInstance, taskState: {}", task.getState())
                            retryTaskInstance(task)
                        }
                        continue
                    }
                    if (task.getState().isFailure()) {
                        errorTaskMap[task.getTaskCode()] = task.getId()
                    }
                } finally {
                    LoggerUtils.removeWorkflowAndTaskInstanceIdMDC()
                }
            }
        } else {
            logger.info("The current workflowInstance is a newly running workflowInstance")
        }
        if (processInstance.isComplementData() && complementListDate.isEmpty()) {
            val cmdParam: Map<String, String> = JSONUtils.toMap(processInstance.getCommandParam())
            if (cmdParam != null) {
                // reset global params while there are start parameters
                setGlobalParamIfCommanded(processDefinition, cmdParam)
                var start: Date? = null
                var end: Date? = null
                if (cmdParam.containsKey(CMD_PARAM_COMPLEMENT_DATA_START_DATE)
                        && cmdParam.containsKey(CMD_PARAM_COMPLEMENT_DATA_END_DATE)) {
                    start = DateUtils.stringToDate(cmdParam[CMD_PARAM_COMPLEMENT_DATA_START_DATE])
                    end = DateUtils.stringToDate(cmdParam[CMD_PARAM_COMPLEMENT_DATA_END_DATE])
                }
                if (complementListDate.isEmpty() && needComplementProcess()) {
                    if (start != null && end != null) {
                        val schedules: List<Schedule> = processService.queryReleaseSchedulerListByProcessDefinitionCode(
                                processInstance.getProcessDefinitionCode())
                        complementListDate = CronUtils.getSelfFireDateList(start, end, schedules)
                    }
                    if (cmdParam.containsKey(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST)) {
                        complementListDate = CronUtils.getSelfScheduleDateList(cmdParam)
                    }
                    logger.info(" process definition code:{} complement data: {}",
                            processInstance.getProcessDefinitionCode(), complementListDate)
                    if (!complementListDate.isEmpty() && Flag.NO === processInstance.getIsSubProcess()) {
                        processInstance.setScheduleTime(complementListDate[0])
                        val globalParams: String = curingParamsService.curingGlobalParams(processInstance.getId(),
                                processDefinition.getGlobalParamMap(),
                                processDefinition.getGlobalParamList(),
                                CommandType.COMPLEMENT_DATA,
                                processInstance.getScheduleTime(),
                                cmdParam[Constants.SCHEDULE_TIMEZONE])
                        processInstance.setGlobalParams(globalParams)
                        processInstanceDao.updateProcessInstance(processInstance)
                    }
                }
            }
        }
        logger.info("Initialize task queue, dependFailedTaskSet: {}, completeTaskMap: {}, errorTaskMap: {}",
                dependFailedTaskSet,
                completeTaskMap,
                errorTaskMap)
    }

    /**
     * submit task to execute
     *
     * @param taskInstance task instance
     * @return TaskInstance
     */
    private fun submitTaskExec(taskInstance: TaskInstance): Optional<TaskInstance> {
        return try {
            // package task instance before submit
            processService.packageTaskInstance(taskInstance, processInstance)
            val taskProcessor: ITaskProcessor = TaskProcessorFactory.getTaskProcessor(taskInstance.getTaskType())
            taskProcessor.init(taskInstance, processInstance)
            if (taskInstance.getState().isRunning()
                    && taskProcessor.getType().equalsIgnoreCase(Constants.COMMON_TASK_TYPE)) {
                notifyProcessHostUpdate(taskInstance)
            }
            val submit: Boolean = taskProcessor.action(TaskAction.SUBMIT)
            if (!submit) {
                logger.error("Submit standby task failed!, taskCode: {}, taskName: {}",
                        taskInstance.getTaskCode(),
                        taskInstance.getName())
                return Optional.empty<TaskInstance>()
            }

            // in a dag, only one taskInstance is valid per taskCode, so need to set the old taskInstance invalid
            LoggerUtils.setWorkflowAndTaskInstanceIDMDC(taskInstance.getProcessInstanceId(), taskInstance.getId())
            if (validTaskMap.containsKey(taskInstance.getTaskCode())) {
                val oldTaskInstanceId = validTaskMap[taskInstance.getTaskCode()]!!
                if (taskInstance.getId() !== oldTaskInstanceId) {
                    val oldTaskInstance: TaskInstance? = taskInstanceMap[oldTaskInstanceId]
                    oldTaskInstance.setFlag(Flag.NO)
                    taskInstanceDao.updateTaskInstance(oldTaskInstance)
                    validTaskMap.remove(taskInstance.getTaskCode())
                    activeTaskProcessorMaps.remove(taskInstance.getTaskCode())
                }
            }
            validTaskMap[taskInstance.getTaskCode()] = taskInstance.getId()
            taskInstanceMap[taskInstance.getId()] = taskInstance
            activeTaskProcessorMaps[taskInstance.getTaskCode()] = taskProcessor

            // if we use task group, then need to acquire the task group resource
            // if there is no resource the current task instance will not be dispatched
            // it will be weakup when other tasks release the resource.
            val taskGroupId: Int = taskInstance.getTaskGroupId()
            if (taskGroupId > 0) {
                val acquireTaskGroup: Boolean = processService.acquireTaskGroup(taskInstance.getId(),
                        taskInstance.getName(),
                        taskGroupId,
                        taskInstance.getProcessInstanceId(),
                        taskInstance.getTaskGroupPriority())
                if (!acquireTaskGroup) {
                    logger.info(
                            "Submitted task will not be dispatch right now because the first time to try to acquire" +
                                    " task group failed, taskInstanceName: {}, taskGroupId: {}",
                            taskInstance.getName(), taskGroupId)
                    return Optional.of<TaskInstance>(taskInstance)
                }
            }
            val dispatchSuccess: Boolean = taskProcessor.action(TaskAction.DISPATCH)
            if (!dispatchSuccess) {
                logger.error("Dispatch standby process {} task {} failed", processInstance.getName(),
                        taskInstance.getName())
                return Optional.empty<TaskInstance>()
            }
            taskProcessor.action(TaskAction.RUN)
            stateWheelExecuteThread.addTask4TimeoutCheck(processInstance, taskInstance)
            stateWheelExecuteThread.addTask4StateCheck(processInstance, taskInstance)
            if (taskProcessor.taskInstance().getState().isFinished()) {
                if (processInstance.isBlocked()) {
                    val processBlockEvent: TaskStateEvent = TaskStateEvent.builder()
                            .processInstanceId(processInstance.getId())
                            .taskInstanceId(taskInstance.getId())
                            .status(taskProcessor.taskInstance().getState())
                            .type(StateEventType.PROCESS_BLOCKED)
                            .build()
                    stateEvents.add(processBlockEvent)
                }
                val taskStateChangeEvent: TaskStateEvent = TaskStateEvent.builder()
                        .processInstanceId(processInstance.getId())
                        .taskInstanceId(taskInstance.getId())
                        .status(taskProcessor.taskInstance().getState())
                        .type(StateEventType.TASK_STATE_CHANGE)
                        .build()
                stateEvents.add(taskStateChangeEvent)
            }
            Optional.of<TaskInstance>(taskInstance)
        } catch (e: Exception) {
            logger.error("Submit standby task {} error, taskCode: {}", taskInstance.getName(),
                    taskInstance.getTaskCode(), e)
            Optional.empty<TaskInstance>()
        } finally {
            LoggerUtils.removeWorkflowAndTaskInstanceIdMDC()
        }
    }

    private fun notifyProcessHostUpdate(taskInstance: TaskInstance) {
        if (org.apache.commons.lang3.StringUtils.isEmpty(taskInstance.getHost())) {
            return
        }
        try {
            val hostUpdateCommand = HostUpdateCommand()
            hostUpdateCommand.setProcessHost(masterAddress)
            hostUpdateCommand.setTaskInstanceId(taskInstance.getId())
            val host = Host(taskInstance.getHost())
            nettyExecutorManager.doExecute(host, hostUpdateCommand.convert2Command())
        } catch (e: Exception) {
            // Do we need to catch this exception?
            logger.error("notify process host update", e)
        }
    }

    /**
     * find task instance in db.
     * in case submit more than one same name task in the same time.
     *
     * @param taskCode    task code
     * @param taskVersion task version
     * @return TaskInstance
     */
    private fun findTaskIfExists(taskCode: Long, taskVersion: Int): TaskInstance? {
        val validTaskInstanceList: List<TaskInstance?> = validTaskList
        for (taskInstance in validTaskInstanceList) {
            if (taskInstance.getTaskCode() === taskCode && taskInstance.getTaskDefinitionVersion() === taskVersion) {
                return taskInstance
            }
        }
        return null
    }

    /**
     * encapsulation task, this method will only create a new task instance, the return task instance will not contain id.
     *
     * @param processInstance process instance
     * @param taskNode        taskNode
     * @return TaskInstance
     */
    private fun createTaskInstance(processInstance: ProcessInstance, taskNode: TaskNode): TaskInstance {
        val taskInstance: TaskInstance? = findTaskIfExists(taskNode.getCode(), taskNode.getVersion())
        return if (taskInstance != null) {
            taskInstance
        } else newTaskInstance(processInstance, taskNode)
    }

    /**
     * clone a new taskInstance for retry and reset some logic fields
     *
     * @return taskInstance
     */
    fun cloneRetryTaskInstance(taskInstance: TaskInstance): TaskInstance? {
        val taskNode: TaskNode = dag.getNode(taskInstance.getTaskCode().toString())
        if (taskNode == null) {
            logger.error("Clone retry taskInstance error because taskNode is null, taskCode:{}",
                    taskInstance.getTaskCode())
            return null
        }
        val newTaskInstance: TaskInstance = newTaskInstance(processInstance, taskNode)
        newTaskInstance.setTaskDefine(taskInstance.getTaskDefine())
        newTaskInstance.setProcessDefine(taskInstance.getProcessDefine())
        newTaskInstance.setProcessInstance(processInstance)
        newTaskInstance.setRetryTimes(taskInstance.getRetryTimes() + 1)
        // todo relative funtion: TaskInstance.retryTaskIntervalOverTime
        newTaskInstance.setState(taskInstance.getState())
        newTaskInstance.setEndTime(taskInstance.getEndTime())
        if (taskInstance.getState() === TaskExecutionStatus.NEED_FAULT_TOLERANCE) {
            newTaskInstance.setAppLink(taskInstance.getAppLink())
        }
        return newTaskInstance
    }

    /**
     * clone a new taskInstance for tolerant and reset some logic fields
     *
     * @return taskInstance
     */
    fun cloneTolerantTaskInstance(taskInstance: TaskInstance): TaskInstance? {
        val taskNode: TaskNode = dag.getNode(taskInstance.getTaskCode().toString())
        if (taskNode == null) {
            logger.error("Clone tolerant taskInstance error because taskNode is null, taskCode:{}",
                    taskInstance.getTaskCode())
            return null
        }
        val newTaskInstance: TaskInstance = newTaskInstance(processInstance, taskNode)
        newTaskInstance.setTaskDefine(taskInstance.getTaskDefine())
        newTaskInstance.setProcessDefine(taskInstance.getProcessDefine())
        newTaskInstance.setProcessInstance(processInstance)
        newTaskInstance.setRetryTimes(taskInstance.getRetryTimes())
        newTaskInstance.setState(taskInstance.getState())
        newTaskInstance.setAppLink(taskInstance.getAppLink())
        return newTaskInstance
    }

    /**
     * new a taskInstance
     *
     * @param processInstance process instance
     * @param taskNode task node
     * @return task instance
     */
    fun newTaskInstance(processInstance: ProcessInstance, taskNode: TaskNode): TaskInstance {
        val taskInstance = TaskInstance()
        taskInstance.setTaskCode(taskNode.getCode())
        taskInstance.setTaskDefinitionVersion(taskNode.getVersion())
        // task name
        taskInstance.setName(taskNode.getName())
        // task instance state
        taskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS)
        // process instance id
        taskInstance.setProcessInstanceId(processInstance.getId())
        // task instance type
        taskInstance.setTaskType(taskNode.getType().toUpperCase())
        // task instance whether alert
        taskInstance.setAlertFlag(Flag.NO)

        // task instance start time
        taskInstance.setStartTime(null)

        // task test flag
        taskInstance.setTestFlag(processInstance.getTestFlag())

        // task instance flag
        taskInstance.setFlag(Flag.YES)

        // task dry run flag
        taskInstance.setDryRun(processInstance.getDryRun())

        // task instance retry times
        taskInstance.setRetryTimes(0)

        // max task instance retry times
        taskInstance.setMaxRetryTimes(taskNode.getMaxRetryTimes())

        // retry task instance interval
        taskInstance.setRetryInterval(taskNode.getRetryInterval())

        // set task param
        taskInstance.setTaskParams(taskNode.getTaskParams())

        // set task group and priority
        taskInstance.setTaskGroupId(taskNode.getTaskGroupId())
        taskInstance.setTaskGroupPriority(taskNode.getTaskGroupPriority())

        // set task cpu quota and max memory
        taskInstance.setCpuQuota(taskNode.getCpuQuota())
        taskInstance.setMemoryMax(taskNode.getMemoryMax())

        // task instance priority
        if (taskNode.getTaskInstancePriority() == null) {
            taskInstance.setTaskInstancePriority(Priority.MEDIUM)
        } else {
            taskInstance.setTaskInstancePriority(taskNode.getTaskInstancePriority())
        }
        var processWorkerGroup: String = processInstance.getWorkerGroup()
        processWorkerGroup = if (org.apache.commons.lang3.StringUtils.isBlank(processWorkerGroup)) DEFAULT_WORKER_GROUP else processWorkerGroup
        val taskWorkerGroup = if (org.apache.commons.lang3.StringUtils.isBlank(taskNode.getWorkerGroup())) processWorkerGroup else taskNode.getWorkerGroup()
        val processEnvironmentCode: Long = if (Objects.isNull(processInstance.getEnvironmentCode())) -1 else processInstance.getEnvironmentCode()
        val taskEnvironmentCode = if (Objects.isNull(taskNode.getEnvironmentCode())) processEnvironmentCode else taskNode.getEnvironmentCode()
        if (processWorkerGroup != DEFAULT_WORKER_GROUP && taskWorkerGroup == DEFAULT_WORKER_GROUP) {
            taskInstance.setWorkerGroup(processWorkerGroup)
            taskInstance.setEnvironmentCode(processEnvironmentCode)
        } else {
            taskInstance.setWorkerGroup(taskWorkerGroup)
            taskInstance.setEnvironmentCode(taskEnvironmentCode)
        }
        if (!taskInstance.getEnvironmentCode().equals(-1L)) {
            val environment: Environment = processService.findEnvironmentByCode(taskInstance.getEnvironmentCode())
            if (Objects.nonNull(environment) && org.apache.commons.lang3.StringUtils.isNotEmpty(environment.getConfig())) {
                taskInstance.setEnvironmentConfig(environment.getConfig())
            }
        }
        // delay execution time
        taskInstance.setDelayTime(taskNode.getDelayTime())
        taskInstance.setTaskExecuteType(taskNode.getTaskExecuteType())
        return taskInstance
    }

    fun getPreVarPool(taskInstance: TaskInstance, preTask: Set<String>) {
        val allProperty: MutableMap<String, Property?> = HashMap<String, Property?>()
        val allTaskInstance: MutableMap<String, TaskInstance?> = HashMap<String, TaskInstance?>()
        if (CollectionUtils.isNotEmpty(preTask)) {
            for (preTaskCode in preTask) {
                val taskId = completeTaskMap[preTaskCode.toLong()] ?: continue
                val preTaskInstance: TaskInstance = taskInstanceMap[taskId] ?: continue
                val preVarPool: String = preTaskInstance.getVarPool()
                if (org.apache.commons.lang3.StringUtils.isNotEmpty(preVarPool)) {
                    val properties: List<Property> = JSONUtils.toList(preVarPool, Property::class.java)
                    for (info in properties) {
                        setVarPoolValue(allProperty, allTaskInstance, preTaskInstance, info)
                    }
                }
            }
            if (allProperty.size > 0) {
                taskInstance.setVarPool(JSONUtils.toJsonString(allProperty.values))
            }
        } else {
            if (org.apache.commons.lang3.StringUtils.isNotEmpty(processInstance.getVarPool())) {
                taskInstance.setVarPool(processInstance.getVarPool())
            }
        }
    }

    val allTaskInstances: Collection<Any>
        get() = taskInstanceMap.values

    private fun setVarPoolValue(allProperty: MutableMap<String, Property?>, allTaskInstance: MutableMap<String, TaskInstance?>,
                                preTaskInstance: TaskInstance?, thisProperty: Property) {
        // for this taskInstance all the param in this part is IN.
        thisProperty.setDirect(Direct.IN)
        // get the pre taskInstance Property's name
        val proName: String = thisProperty.getProp()
        // if the Previous nodes have the Property of same name
        if (allProperty.containsKey(proName)) {
            // comparison the value of two Property
            val otherPro: Property? = allProperty[proName]
            // if this property'value of loop is empty,use the other,whether the other's value is empty or not
            if (org.apache.commons.lang3.StringUtils.isEmpty(thisProperty.getValue())) {
                allProperty[proName] = otherPro
                // if property'value of loop is not empty,and the other's value is not empty too, use the earlier value
            } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(otherPro.getValue())) {
                val otherTask: TaskInstance? = allTaskInstance[proName]
                if (otherTask.getEndTime().getTime() > preTaskInstance.getEndTime().getTime()) {
                    allProperty[proName] = thisProperty
                    allTaskInstance[proName] = preTaskInstance
                } else {
                    allProperty[proName] = otherPro
                }
            } else {
                allProperty[proName] = thisProperty
                allTaskInstance[proName] = preTaskInstance
            }
        } else {
            allProperty[proName] = thisProperty
            allTaskInstance[proName] = preTaskInstance
        }
    }

    private val completeTaskInstanceMap: Map<String, Any>
        /**
         * get complete task instance map, taskCode as key
         */
        private get() {
            val completeTaskInstanceMap: MutableMap<String, TaskInstance> = HashMap<String, TaskInstance>()
            for ((taskConde, taskInstanceId) in completeTaskMap) {
                val taskInstance: TaskInstance? = taskInstanceMap[taskInstanceId]
                if (taskInstance == null) {
                    logger.warn("Cannot find the taskInstance from taskInstanceMap, taskInstanceId: {}, taskConde: {}",
                            taskInstanceId,
                            taskConde)
                    // This case will happen when we submit to db failed, then the taskInstanceId is 0
                    continue
                }
                completeTaskInstanceMap[taskInstance.getTaskCode().toString()] = taskInstance
            }
            return completeTaskInstanceMap
        }
    private val validTaskList: List<Any?>
        /**
         * get valid task list
         */
        private get() {
            val validTaskInstanceList: MutableList<TaskInstance?> = ArrayList<TaskInstance?>()
            for (taskInstanceId in validTaskMap.values) {
                validTaskInstanceList.add(taskInstanceMap[taskInstanceId])
            }
            return validTaskInstanceList
        }

    @Throws(StateEventHandleException::class)
    private fun submitPostNode(parentNodeCode: String?) {
        val submitTaskNodeList: Set<String> = DagHelper.parsePostNodes(parentNodeCode, skipTaskNodeMap, dag, completeTaskInstanceMap)
        val taskInstances: MutableList<TaskInstance> = ArrayList<TaskInstance>()
        for (taskNode in submitTaskNodeList) {
            val taskNodeObject: TaskNode = dag.getNode(taskNode)
            val existTaskInstanceOptional: Optional<TaskInstance> = getTaskInstance(taskNodeObject.getCode())
            if (existTaskInstanceOptional.isPresent()) {
                taskInstances.add(existTaskInstanceOptional.get())
                continue
            }
            val task: TaskInstance = createTaskInstance(processInstance, taskNodeObject)
            taskInstances.add(task)
        }
        // the end node of the branch of the dag
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(parentNodeCode) && dag.getEndNode().contains(parentNodeCode)) {
            val endTaskInstance: TaskInstance? = taskInstanceMap[completeTaskMap[org.apache.commons.lang3.math.NumberUtils.toLong(parentNodeCode)]]
            val taskInstanceVarPool: String = endTaskInstance.getVarPool()
            if (org.apache.commons.lang3.StringUtils.isNotEmpty(taskInstanceVarPool)) {
                val taskProperties: Set<Property> = HashSet<Any?>(JSONUtils.toList(taskInstanceVarPool, Property::class.java))
                val processInstanceVarPool: String = processInstance.getVarPool()
                if (org.apache.commons.lang3.StringUtils.isNotEmpty(processInstanceVarPool)) {
                    val properties: MutableSet<Property> = HashSet<Any?>(JSONUtils.toList(processInstanceVarPool, Property::class.java))
                    properties.addAll(taskProperties)
                    processInstance.setVarPool(JSONUtils.toJsonString(properties))
                } else {
                    processInstance.setVarPool(JSONUtils.toJsonString(taskProperties))
                }
            }
        }

        // if previous node success , post node submit
        for (task in taskInstances) {
            if (readyToSubmitTaskQueue.contains(task)) {
                logger.warn("Task is already at submit queue, taskInstanceId: {}", task.getId())
                continue
            }
            if (task.getId() != null && completeTaskMap.containsKey(task.getTaskCode())) {
                logger.info("Task has already run success, taskName: {}", task.getName())
                continue
            }
            if (task.getState().isKill()) {
                logger.info("Task is be stopped, the state is {}, taskInstanceId: {}", task.getState(), task.getId())
                continue
            }
            addTaskToStandByList(task)
        }
        submitStandByTask()
        updateProcessInstanceState()
    }

    /**
     * determine whether the dependencies of the task node are complete
     *
     * @return DependResult
     */
    private fun isTaskDepsComplete(taskCode: String): DependResult {
        val startNodes: Collection<String> = dag.getBeginNode()
        // if vertex,returns true directly
        if (startNodes.contains(taskCode)) {
            return DependResult.SUCCESS
        }
        val taskNode: TaskNode = dag.getNode(taskCode)
        val indirectDepCodeList: MutableList<String> = ArrayList()
        setIndirectDepList(taskCode, indirectDepCodeList)
        for (depsNode in indirectDepCodeList) {
            if (dag.containsNode(depsNode) && !skipTaskNodeMap.containsKey(depsNode)) {
                // dependencies must be fully completed
                val despNodeTaskCode = depsNode.toLong()
                if (!completeTaskMap.containsKey(despNodeTaskCode)) {
                    return DependResult.WAITING
                }
                val depsTaskId = completeTaskMap[despNodeTaskCode]
                val depTaskState: TaskExecutionStatus = taskInstanceMap[depsTaskId].getState()
                if (depTaskState.isKill()) {
                    return DependResult.NON_EXEC
                }
                // ignore task state if current task is block
                if (taskNode.isBlockingTask()) {
                    continue
                }

                // always return success if current task is condition
                if (taskNode.isConditionsTask()) {
                    continue
                }
                if (!dependTaskSuccess(depsNode, taskCode)) {
                    return DependResult.FAILED
                }
            }
        }
        logger.info("The dependTasks of task all success, currentTaskCode: {}, dependTaskCodes: {}",
                taskCode, completeTaskMap.keys.toTypedArray().contentToString())
        return DependResult.SUCCESS
    }

    /**
     * This function is specially used to handle the dependency situation where the parent node is a prohibited node.
     * When the parent node is a forbidden node, the dependency relationship should continue to be traced
     *
     * @param taskCode            taskCode
     * @param indirectDepCodeList All indirectly dependent nodes
     */
    private fun setIndirectDepList(taskCode: String, indirectDepCodeList: MutableList<String>) {
        val taskNode: TaskNode = dag.getNode(taskCode)
        val depCodeList: List<String> = taskNode.getDepList()
        for (depsNode in depCodeList) {
            if (forbiddenTaskMap.containsKey(depsNode.toLong())) {
                setIndirectDepList(depsNode, indirectDepCodeList)
            } else {
                indirectDepCodeList.add(depsNode)
            }
        }
    }

    /**
     * depend node is completed, but here need check the condition task branch is the next node
     */
    private fun dependTaskSuccess(dependNodeName: String, nextNodeName: String): Boolean {
        if (dag.getNode(dependNodeName).isConditionsTask()) {
            // condition task need check the branch to run
            val nextTaskList: List<String> = DagHelper.parseConditionTask(dependNodeName, skipTaskNodeMap, dag, completeTaskInstanceMap)
            if (!nextTaskList.contains(nextNodeName)) {
                logger.info(
                        "DependTask is a condition task, and its next condition branch does not hava current task, " +
                                "dependTaskCode: {}, currentTaskCode: {}",
                        dependNodeName, nextNodeName)
                return false
            }
        } else {
            val taskCode = dependNodeName.toLong()
            val taskInstanceId = completeTaskMap[taskCode]
            val depTaskState: TaskExecutionStatus = taskInstanceMap[taskInstanceId].getState()
            return !depTaskState.isFailure()
        }
        return true
    }

    /**
     * query task instance by complete state
     *
     * @param state state
     * @return task instance list
     */
    private fun getCompleteTaskByState(state: TaskExecutionStatus): List<TaskInstance> {
        val resultList: MutableList<TaskInstance> = ArrayList<TaskInstance>()
        for (taskInstanceId in completeTaskMap.values) {
            val taskInstance: TaskInstance? = taskInstanceMap[taskInstanceId]
            if (taskInstance != null && taskInstance.getState() === state) {
                resultList.add(taskInstance)
            }
        }
        return resultList
    }

    /**
     * where there are ongoing tasks
     *
     * @param state state
     * @return ExecutionStatus
     */
    private fun runningState(state: WorkflowExecutionStatus): WorkflowExecutionStatus {
        return if (state === WorkflowExecutionStatus.READY_STOP || state === WorkflowExecutionStatus.READY_PAUSE || state === WorkflowExecutionStatus.READY_BLOCK || state === WorkflowExecutionStatus.DELAY_EXECUTION) {
            // if the running task is not completed, the state remains unchanged
            state
        } else {
            WorkflowExecutionStatus.RUNNING_EXECUTION
        }
    }

    /**
     * exists failure task,contains submit failure、dependency failure,execute failure(retry after)
     *
     * @return Boolean whether has failed task
     */
    private fun hasFailedTask(): Boolean {
        if (taskFailedSubmit) {
            return true
        }
        return if (errorTaskMap.size > 0) {
            true
        } else dependFailedTaskSet.size > 0
    }

    /**
     * process instance failure
     *
     * @return Boolean whether process instance failed
     */
    private fun processFailed(): Boolean {
        if (hasFailedTask()) {
            logger.info("The current process has failed task, the current process failed")
            if (processInstance.getFailureStrategy() === FailureStrategy.END) {
                return true
            }
            if (processInstance.getFailureStrategy() === FailureStrategy.CONTINUE) {
                return readyToSubmitTaskQueue.size() === 0 && activeTaskProcessorMaps.size == 0 && waitToRetryTaskInstanceMap.size == 0
            }
        }
        return false
    }

    /**
     * prepare for pause
     * 1，failed retry task in the preparation queue , returns to failure directly
     * 2，exists pause task，complement not completed, pending submission of tasks, return to suspension
     * 3，success
     *
     * @return ExecutionStatus
     */
    private fun processReadyPause(): WorkflowExecutionStatus {
        if (hasRetryTaskInStandBy()) {
            return WorkflowExecutionStatus.FAILURE
        }
        val pauseList: List<TaskInstance> = getCompleteTaskByState(TaskExecutionStatus.PAUSE)
        return if (CollectionUtils.isNotEmpty(pauseList) || processInstance.isBlocked() || !isComplementEnd || readyToSubmitTaskQueue.size() > 0) {
            WorkflowExecutionStatus.PAUSE
        } else {
            WorkflowExecutionStatus.SUCCESS
        }
    }

    /**
     * prepare for block
     * if process has tasks still running, pause them
     * if readyToSubmitTaskQueue is not empty, kill them
     * else return block status directly
     *
     * @return ExecutionStatus
     */
    private fun processReadyBlock(): WorkflowExecutionStatus {
        if (activeTaskProcessorMaps.size > 0) {
            for (taskProcessor in activeTaskProcessorMaps.values) {
                if (!TASK_TYPE_BLOCKING.equals(taskProcessor.getType())) {
                    taskProcessor.action(TaskAction.PAUSE)
                }
            }
        }
        if (readyToSubmitTaskQueue.size() > 0) {
            val iter: Iterator<TaskInstance> = readyToSubmitTaskQueue.iterator()
            while (iter.hasNext()) {
                iter.next().setState(TaskExecutionStatus.PAUSE)
            }
        }
        return WorkflowExecutionStatus.BLOCK
    }

    /**
     * generate the latest process instance status by the tasks state
     *
     * @return process instance execution status
     */
    private fun getProcessInstanceState(instance: ProcessInstance): WorkflowExecutionStatus {
        val state: WorkflowExecutionStatus = instance.getState()
        if (activeTaskProcessorMaps.size > 0 || hasRetryTaskInStandBy()) {
            // active task and retry task exists
            val executionStatus: WorkflowExecutionStatus = runningState(state)
            logger.info("The workflowInstance has task running, the workflowInstance status is {}", executionStatus)
            return executionStatus
        }

        // block
        if (state === WorkflowExecutionStatus.READY_BLOCK) {
            val executionStatus: WorkflowExecutionStatus = processReadyBlock()
            logger.info("The workflowInstance is ready to block, the workflowInstance status is {}", executionStatus)
            return executionStatus
        }

        // pause
        if (state === WorkflowExecutionStatus.READY_PAUSE) {
            val executionStatus: WorkflowExecutionStatus = processReadyPause()
            logger.info("The workflowInstance is ready to pause, the workflow status is {}", executionStatus)
            return executionStatus
        }

        // stop
        if (state === WorkflowExecutionStatus.READY_STOP) {
            val killList: List<TaskInstance> = getCompleteTaskByState(TaskExecutionStatus.KILL)
            val failList: List<TaskInstance> = getCompleteTaskByState(TaskExecutionStatus.FAILURE)
            val executionStatus: WorkflowExecutionStatus
            executionStatus = if (CollectionUtils.isNotEmpty(killList) || CollectionUtils.isNotEmpty(failList) || !isComplementEnd) {
                WorkflowExecutionStatus.STOP
            } else {
                WorkflowExecutionStatus.SUCCESS
            }
            logger.info("The workflowInstance is ready to stop, the workflow status is {}", executionStatus)
            return executionStatus
        }

        // process failure
        if (processFailed()) {
            logger.info("The workflowInstance is failed, the workflow status is {}", WorkflowExecutionStatus.FAILURE)
            return WorkflowExecutionStatus.FAILURE
        }

        // success
        if (state === WorkflowExecutionStatus.RUNNING_EXECUTION) {
            val killTasks: List<TaskInstance> = getCompleteTaskByState(TaskExecutionStatus.KILL)
            return if (readyToSubmitTaskQueue.size() > 0 || waitToRetryTaskInstanceMap.size > 0) {
                // tasks currently pending submission, no retries, indicating that depend is waiting to complete
                WorkflowExecutionStatus.RUNNING_EXECUTION
            } else if (CollectionUtils.isNotEmpty(killTasks)) {
                // tasks maybe killed manually
                WorkflowExecutionStatus.FAILURE
            } else {
                // if the waiting queue is empty and the status is in progress, then success
                WorkflowExecutionStatus.SUCCESS
            }
        }
        return state
    }

    private val isComplementEnd: Boolean
        /**
         * whether complement end
         *
         * @return Boolean whether is complement end
         */
        private get() {
            if (!processInstance.isComplementData()) {
                return true
            }
            val cmdParam: Map<String, String> = JSONUtils.toMap(processInstance.getCommandParam())
            val endTime: Date = DateUtils.stringToDate(cmdParam[CMD_PARAM_COMPLEMENT_DATA_END_DATE])
            return processInstance.getScheduleTime().equals(endTime)
        }

    /**
     * updateProcessInstance process instance state
     * after each batch of tasks is executed, the status of the process instance is updated
     */
    @Throws(StateEventHandleException::class)
    private fun updateProcessInstanceState() {
        val state: WorkflowExecutionStatus = getProcessInstanceState(processInstance)
        if (processInstance.getState() !== state) {
            logger.info("Update workflowInstance states, origin state: {}, target state: {}",
                    processInstance.getState(),
                    state)
            updateWorkflowInstanceStatesToDB(state)
            val stateEvent: WorkflowStateEvent = WorkflowStateEvent.builder()
                    .processInstanceId(processInstance.getId())
                    .status(processInstance.getState())
                    .type(StateEventType.PROCESS_STATE_CHANGE)
                    .build()
            // replace with `stateEvents`, make sure `WorkflowExecuteThread` can be deleted to avoid memory leaks
            stateEvents.add(stateEvent)
        } else {
            logger.info("There is no need to update the workflow instance state, origin state: {}, target state: {}",
                    processInstance.getState(),
                    state)
        }
    }

    /**
     * stateEvent's execution status as process instance state
     */
    @Throws(StateEventHandleException::class)
    fun updateProcessInstanceState(stateEvent: WorkflowStateEvent) {
        val state: WorkflowExecutionStatus = stateEvent.getStatus()
        updateWorkflowInstanceStatesToDB(state)
    }

    @Throws(StateEventHandleException::class)
    private fun updateWorkflowInstanceStatesToDB(newStates: WorkflowExecutionStatus) {
        val originStates: WorkflowExecutionStatus = processInstance.getState()
        if (originStates !== newStates) {
            logger.info("Begin to update workflow instance state , state will change from {} to {}",
                    originStates,
                    newStates)
            processInstance.setStateWithDesc(newStates, "update by workflow executor")
            if (newStates.isFinished()) {
                processInstance.setEndTime(Date())
            }
            try {
                processInstanceDao.updateProcessInstance(processInstance)
            } catch (ex: Exception) {
                // recover the status
                processInstance.setStateWithDesc(originStates, "recover state by DB error")
                processInstance.setEndTime(null)
                throw StateEventHandleException("Update process instance status to DB error", ex)
            }
        }
    }

    /**
     * get task dependency result
     *
     * @param taskInstance task instance
     * @return DependResult
     */
    private fun getDependResultForTask(taskInstance: TaskInstance): DependResult {
        return isTaskDepsComplete(taskInstance.getTaskCode().toString())
    }

    /**
     * add task to standby list
     *
     * @param taskInstance task instance
     */
    fun addTaskToStandByList(taskInstance: TaskInstance?) {
        if (readyToSubmitTaskQueue.contains(taskInstance)) {
            logger.warn("Task already exists in ready submit queue, no need to add again, task code:{}",
                    taskInstance.getTaskCode())
            return
        }
        logger.info("Add task to stand by list, task name:{}, task id:{}, task code:{}",
                taskInstance.getName(),
                taskInstance.getId(),
                taskInstance.getTaskCode())
        TaskMetrics.incTaskInstanceByState("submit")
        readyToSubmitTaskQueue.put(taskInstance)
    }

    /**
     * remove task from stand by list
     *
     * @param taskInstance task instance
     */
    private fun removeTaskFromStandbyList(taskInstance: TaskInstance): Boolean {
        return readyToSubmitTaskQueue.remove(taskInstance)
    }

    /**
     * has retry task in standby
     *
     * @return Boolean whether has retry task in standby
     */
    private fun hasRetryTaskInStandBy(): Boolean {
        val iter: Iterator<TaskInstance> = readyToSubmitTaskQueue.iterator()
        while (iter.hasNext()) {
            if (iter.next().getState().isFailure()) {
                return true
            }
        }
        return false
    }

    /**
     * close the on going tasks
     */
    fun killAllTasks() {
        logger.info("kill called on process instance id: {}, num: {}",
                processInstance.getId(),
                activeTaskProcessorMaps.size)
        if (readyToSubmitTaskQueue.size() > 0) {
            readyToSubmitTaskQueue.clear()
        }
        for (taskCode in activeTaskProcessorMaps.keys) {
            val taskProcessor: ITaskProcessor? = activeTaskProcessorMaps[taskCode]
            val taskInstanceId = validTaskMap[taskCode]
            if (taskInstanceId == null || taskInstanceId == 0) {
                continue
            }
            val taskInstance: TaskInstance = taskInstanceDao.findTaskInstanceById(taskInstanceId)
            if (taskInstance == null || taskInstance.getState().isFinished()) {
                continue
            }
            taskProcessor.action(TaskAction.STOP)
            if (taskProcessor.taskInstance().getState().isFinished()) {
                val taskStateEvent: TaskStateEvent = TaskStateEvent.builder()
                        .processInstanceId(processInstance.getId())
                        .taskInstanceId(taskInstance.getId())
                        .status(taskProcessor.taskInstance().getState())
                        .type(StateEventType.TASK_STATE_CHANGE)
                        .build()
                addStateEvent(taskStateEvent)
            }
        }
    }

    fun workFlowFinish(): Boolean {
        return processInstance.getState().isFinished()
    }

    /**
     * handling the list of tasks to be submitted
     */
    @Throws(StateEventHandleException::class)
    fun submitStandByTask() {
        val length: Int = readyToSubmitTaskQueue.size()
        for (i in 0 until length) {
            val task: TaskInstance = readyToSubmitTaskQueue.peek() ?: continue
            // stop tasks which is retrying if forced success happens
            if (task.taskCanRetry()) {
                val retryTask: TaskInstance = taskInstanceDao.findTaskInstanceById(task.getId())
                if (retryTask != null && retryTask.getState().isForceSuccess()) {
                    task.setState(retryTask.getState())
                    logger.info(
                            "Task {} has been forced success, put it into complete task list and stop retrying, taskInstanceId: {}",
                            task.getName(), task.getId())
                    removeTaskFromStandbyList(task)
                    completeTaskMap[task.getTaskCode()] = task.getId()
                    taskInstanceMap[task.getId()] = task
                    submitPostNode(task.getTaskCode().toString())
                    continue
                }
            }
            // init varPool only this task is the first time running
            if (task.isFirstRun()) {
                // get pre task ,get all the task varPool to this task
                val preTask: Set<String> = dag.getPreviousNodes(task.getTaskCode().toString())
                getPreVarPool(task, preTask)
            }
            val dependResult: DependResult = getDependResultForTask(task)
            if (DependResult.SUCCESS === dependResult) {
                logger.info("The dependResult of task {} is success, so ready to submit to execute", task.getName())
                val taskInstanceOptional: Optional<TaskInstance> = submitTaskExec(task)
                if (!taskInstanceOptional.isPresent()) {
                    taskFailedSubmit = true
                    // Remove and add to complete map and error map
                    if (!removeTaskFromStandbyList(task)) {
                        logger.error(
                                "Task submit failed, remove from standby list failed, workflowInstanceId: {}, taskCode: {}",
                                processInstance.getId(),
                                task.getTaskCode())
                    }
                    completeTaskMap[task.getTaskCode()] = task.getId()
                    taskInstanceMap[task.getId()] = task
                    errorTaskMap[task.getTaskCode()] = task.getId()
                    activeTaskProcessorMaps.remove(task.getTaskCode())
                    logger.error("Task submitted failed, workflowInstanceId: {}, taskInstanceId: {}, taskCode: {}",
                            task.getProcessInstanceId(),
                            task.getId(),
                            task.getTaskCode())
                } else {
                    removeTaskFromStandbyList(task)
                }
            } else if (DependResult.FAILED === dependResult) {
                // if the dependency fails, the current node is not submitted and the state changes to failure.
                dependFailedTaskSet.add(task.getTaskCode())
                removeTaskFromStandbyList(task)
                logger.info("Task dependent result is failed, taskInstanceId:{} depend result : {}", task.getId(),
                        dependResult)
            } else if (DependResult.NON_EXEC === dependResult) {
                // for some reasons(depend task pause/stop) this task would not be submit
                removeTaskFromStandbyList(task)
                logger.info("Remove task due to depend result not executed, taskInstanceId:{} depend result : {}",
                        task.getId(), dependResult)
            }
        }
    }

    /**
     * Get start task instance list from recover
     *
     * @param cmdParam command param
     * @return task instance list
     */
    protected fun getRecoverTaskInstanceList(cmdParam: String?): List<TaskInstance> {
        val paramMap: Map<String, String> = JSONUtils.toMap(cmdParam)

        // todo: Can we use a better way to set the recover taskInstanceId list? rather then use the cmdParam
        if (paramMap != null && paramMap.containsKey(CMD_PARAM_RECOVERY_START_NODE_STRING)) {
            val startTaskInstanceIds = Arrays.stream<String>(paramMap[CMD_PARAM_RECOVERY_START_NODE_STRING]
                    .split(COMMA.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
                    .filter { cs: String? -> org.apache.commons.lang3.StringUtils.isNotEmpty(cs) }
                    .map<Int?>(Function<String, Int?> { s: String? -> s })
                    .collect(Collectors.toList<Int?>())
            if (CollectionUtils.isNotEmpty(startTaskInstanceIds)) {
                return taskInstanceDao.findTaskInstanceByIdList(startTaskInstanceIds)
            }
        }
        return emptyList<TaskInstance>()
    }

    /**
     * parse "StartNodeNameList" from cmd param
     *
     * @param cmdParam command param
     * @return start node name list
     */
    private fun parseStartNodeName(cmdParam: String): List<String> {
        var startNodeNameList: List<String> = ArrayList()
        val paramMap: Map<String, String> = JSONUtils.toMap(cmdParam)
                ?: return startNodeNameList
        if (paramMap.containsKey(CMD_PARAM_START_NODES)) {
            startNodeNameList = Arrays.asList<String>(*paramMap[CMD_PARAM_START_NODES].split(Constants.COMMA.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        }
        return startNodeNameList
    }

    /**
     * generate start node code list from parsing command param;
     * if "StartNodeIdList" exists in command param, return StartNodeIdList
     *
     * @return recovery node code list
     */
    private fun getRecoveryNodeCodeList(recoverNodeList: List<TaskInstance>): List<String> {
        val recoveryNodeCodeList: MutableList<String> = ArrayList()
        if (CollectionUtils.isNotEmpty(recoverNodeList)) {
            for (task in recoverNodeList) {
                recoveryNodeCodeList.add(task.getTaskCode().toString())
            }
        }
        return recoveryNodeCodeList
    }

    /**
     * generate flow dag
     *
     * @param totalTaskNodeList    total task node list
     * @param startNodeNameList    start node name list
     * @param recoveryNodeCodeList recovery node code list
     * @param depNodeType          depend node type
     * @return ProcessDag           process dag
     * @throws Exception exception
     */
    @Throws(Exception::class)
    fun generateFlowDag(totalTaskNodeList: List<TaskNode?>?,
                        startNodeNameList: List<String>?,
                        recoveryNodeCodeList: List<String>?,
                        depNodeType: TaskDependType?): ProcessDag {
        return DagHelper.generateFlowDag(totalTaskNodeList, startNodeNameList, recoveryNodeCodeList, depNodeType)
    }

    /**
     * check task queue
     */
    private fun checkTaskQueue(): Boolean {
        val result = AtomicBoolean(false)
        taskInstanceMap.forEach(BiConsumer<Int, TaskInstance> { id: Int?, taskInstance: TaskInstance? ->
            if (taskInstance != null && taskInstance.getTaskGroupId() > 0) {
                result.set(true)
            }
        })
        return result.get()
    }

    private val isNewProcessInstance: Boolean
        /**
         * is new process instance
         */
        private get() {
            if (Flag.YES.equals(processInstance.getRecovery())) {
                logger.info("This workInstance will be recover by this execution")
                return false
            }
            if (WorkflowExecutionStatus.RUNNING_EXECUTION === processInstance.getState()
                    && processInstance.getRunTimes() === 1) {
                return true
            }
            logger.info(
                    "The workflowInstance has been executed before, this execution is to reRun, processInstance status: {}, runTimes: {}",
                    processInstance.getState(),
                    processInstance.getRunTimes())
            return false
        }

    @Throws(Exception::class)
    fun resubmit(taskCode: Long) {
        val taskProcessor: ITaskProcessor? = activeTaskProcessorMaps[taskCode]
        if (taskProcessor != null) {
            taskProcessor.action(TaskAction.RESUBMIT)
            logger.debug("RESUBMIT: task code:{}", taskCode)
        } else {
            throw Exception("resubmit error, taskProcessor is null, task code: $taskCode")
        }
    }

    fun getCompleteTaskMap(): Map<Long, Int> {
        return completeTaskMap
    }

    val activeTaskProcessMap: Map<Long, Any>
        get() = activeTaskProcessorMaps

    fun getWaitToRetryTaskInstanceMap(): Map<Long, TaskInstance> {
        return waitToRetryTaskInstanceMap
    }

    private fun setGlobalParamIfCommanded(processDefinition: ProcessDefinition?, cmdParam: Map<String, String>) {
        // get start params from command param
        var startParamMap: MutableMap<String?, String?> = HashMap()
        if (cmdParam.containsKey(CMD_PARAM_START_PARAMS)) {
            val startParamJson = cmdParam[CMD_PARAM_START_PARAMS]
            startParamMap = JSONUtils.toMap(startParamJson)
        }
        var fatherParamMap: Map<String?, String?> = HashMap()
        if (cmdParam.containsKey(CMD_PARAM_FATHER_PARAMS)) {
            val fatherParamJson = cmdParam[CMD_PARAM_FATHER_PARAMS]
            fatherParamMap = JSONUtils.toMap(fatherParamJson)
        }
        startParamMap.putAll(fatherParamMap)
        // set start param into global params
        val globalMap: MutableMap<String?, String?> = processDefinition.getGlobalParamMap()
        val globalParamList: MutableList<Property> = processDefinition.getGlobalParamList()
        if (startParamMap.size > 0 && globalMap != null) {
            // start param to overwrite global param
            for (param in globalMap.entries) {
                val `val` = startParamMap[param.key]
                if (`val` != null) {
                    param.setValue(`val`)
                }
            }
            // start param to create new global param if global not exist
            for ((key1, value) in startParamMap) {
                if (!globalMap.containsKey(key1)) {
                    globalMap[key1] = value
                    globalParamList.add(Property(key1, IN, VARCHAR, value))
                }
            }
        }
    }

    private enum class WorkflowRunnableStatus {
        CREATED,
        INITIALIZE_DAG,
        INITIALIZE_QUEUE,
        STARTED
    }

    companion object {
        private val logger = LoggerFactory.getLogger(WorkflowExecuteRunnable::class.java)
    }
}
