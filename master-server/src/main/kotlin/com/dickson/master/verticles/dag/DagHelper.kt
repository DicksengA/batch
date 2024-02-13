package com.dickson.master.verticles.dag

import com.dickson.common.DAG
import com.dickson.common.enums.TaskDependType
import com.dickson.master.verticles.*
import org.slf4j.LoggerFactory
import java.util.*
import java.util.function.Consumer
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
 * dag tools
 */
object DagHelper {
    private val logger = LoggerFactory.getLogger(DagHelper::class.java)

    /**
     * generate flow node relation list by task node list;
     * Edges that are not in the task Node List will not be added to the result
     *
     * @param taskNodeList taskNodeList
     * @return task node relation list
     */
    fun generateRelationListByFlowNodes(taskNodeList: List<TaskNode>): List<TaskNodeRelation> {
        val nodeRelationList: MutableList<TaskNodeRelation> = ArrayList()
        for (taskNode in taskNodeList) {
            val preTasks: String = taskNode.getPreTasks()
            val preTaskList: List<String> = JSONUtils.toList(preTasks, String::class.java)
            if (preTaskList != null) {
                for (depNodeCode in preTaskList) {
                    if (null != findNodeByCode(taskNodeList, depNodeCode)) {
                        nodeRelationList.add(TaskNodeRelation(depNodeCode, taskNode.getCode().toString()))
                    }
                }
            }
        }
        return nodeRelationList
    }

    /**
     * generate task nodes needed by dag
     *
     * @param taskNodeList taskNodeList
     * @param startNodeNameList startNodeNameList
     * @param recoveryNodeCodeList recoveryNodeCodeList
     * @param taskDependType taskDependType
     * @return task node list
     */
    fun generateFlowNodeListByStartNode(taskNodeList: MutableList<TaskNode>,
                                        startNodeNameList: List<String>,
                                        recoveryNodeCodeList: List<String>,
                                        taskDependType: TaskDependType): List<TaskNode> {
        val destFlowNodeList: List<TaskNode> = ArrayList()
        var startNodeList = startNodeNameList
        if (taskDependType !== TaskDependType.TASK_POST && startNodeList.isEmpty()) {
            logger.error("start node list is empty! cannot continue run the process ")
            return destFlowNodeList
        }
        val destTaskNodeList: MutableList<TaskNode> = ArrayList()
        var tmpTaskNodeList: MutableList<TaskNode> = ArrayList()
        if (taskDependType === TaskDependType.TASK_POST
                && recoveryNodeCodeList.isNotEmpty()) {
            startNodeList = recoveryNodeCodeList
        }
        if (startNodeList.isEmpty()) {
            // no special designation start nodes
            tmpTaskNodeList = taskNodeList
        } else {
            // specified start nodes or resume execution
            for (startNodeCode in startNodeList) {
                val startNode = findNodeByCode(taskNodeList, startNodeCode)
                var childNodeList: ArrayList<TaskNode> = ArrayList()
                if (startNode == null) {
                    logger.error("start node name [{}] is not in task node list [{}] ",
                            startNodeCode,
                            taskNodeList)
                    continue
                } else if (TaskDependType.TASK_POST === taskDependType) {
                    val visitedNodeCodeList: MutableList<String> = ArrayList()
                    childNodeList = getFlowNodeListPost(startNode, taskNodeList, visitedNodeCodeList)
                } else if (TaskDependType.TASK_PRE === taskDependType) {
                    val visitedNodeCodeList: MutableList<String> = ArrayList()
                    childNodeList = getFlowNodeListPre(startNode, recoveryNodeCodeList, taskNodeList, visitedNodeCodeList)
                } else {
                    childNodeList.add(startNode)
                }
                tmpTaskNodeList.addAll(childNodeList)
            }
        }
        for (taskNode in tmpTaskNodeList) {
            if (null == findNodeByCode(destTaskNodeList, taskNode.getCode().toString())) {
                destTaskNodeList.add(taskNode)
            }
        }
        return destTaskNodeList
    }

    /**
     * find all the nodes that depended on the start node
     *
     * @param startNode startNode
     * @param taskNodeList taskNodeList
     * @return task node list
     */
    private fun getFlowNodeListPost(startNode: TaskNode, taskNodeList: List<TaskNode>,
                                    visitedNodeCodeList: MutableList<String>): ArrayList<TaskNode> {
        val resultList: ArrayList<TaskNode> = ArrayList()
        for (taskNode in taskNodeList) {
            val depList: List<String> = taskNode.getDepList()
            if (null != depList && null != startNode && depList.contains(startNode.getCode().toString())
                    && !visitedNodeCodeList.contains(taskNode.getCode().toString())) {
                resultList.addAll(getFlowNodeListPost(taskNode, taskNodeList, visitedNodeCodeList))
            }
        }
        // why add (startNode != null) condition? for SonarCloud Quality Gate passed
        if (null != startNode) {
            visitedNodeCodeList.add(startNode.getCode().toString())
        }
        resultList.add(startNode)
        return resultList
    }

    /**
     * find all nodes that start nodes depend on.
     *
     * @param startNode startNode
     * @param recoveryNodeCodeList recoveryNodeCodeList
     * @param taskNodeList taskNodeList
     * @return task node list
     */
    private fun getFlowNodeListPre(startNode: TaskNode, recoveryNodeCodeList: List<String>,
                                   taskNodeList: List<TaskNode>, visitedNodeCodeList: MutableList<String>): ArrayList<TaskNode> {
        val resultList: ArrayList<TaskNode> = ArrayList()
        var depList: List<String> = ArrayList()
        if (null != startNode) {
            depList = startNode.getDepList()
            resultList.add(startNode)
        }
        if (depList.isEmpty()) {
            return resultList
        }
        for (depNodeCode in depList) {
            val start = findNodeByCode(taskNodeList, depNodeCode)
            if (recoveryNodeCodeList.contains(depNodeCode) && start !=null) {
                resultList.add(start)
            } else if (!visitedNodeCodeList.contains(depNodeCode) && start != null) {
                resultList.addAll(getFlowNodeListPre(start, recoveryNodeCodeList, taskNodeList, visitedNodeCodeList))
            }
        }
        // why add (startNode != null) condition? for SonarCloud Quality Gate passed
        if (null != startNode) {
            visitedNodeCodeList.add(startNode.getCode().toString())
        }
        return resultList
    }

    /**
     * generate dag by start nodes and recovery nodes
     *
     * @param totalTaskNodeList totalTaskNodeList
     * @param startNodeNameList startNodeNameList
     * @param recoveryNodeCodeList recoveryNodeCodeList
     * @param depNodeType depNodeType
     * @return process dag
     * @throws Exception if error throws Exception
     */
    @Throws(Exception::class)
    fun generateFlowDag(totalTaskNodeList: MutableList<TaskNode>,
                        startNodeNameList: List<String>,
                        recoveryNodeCodeList: List<String>,
                        depNodeType: TaskDependType): ProcessDag? {
        val destTaskNodeList = generateFlowNodeListByStartNode(totalTaskNodeList, startNodeNameList,
                recoveryNodeCodeList, depNodeType)
        if (destTaskNodeList.isEmpty()) {
            return null
        }
        val taskNodeRelations = generateRelationListByFlowNodes(destTaskNodeList)
        val processDag = ProcessDag()
        processDag.setEdges(taskNodeRelations)
        processDag.setNodes(destTaskNodeList)
        return processDag
    }

    /**
     * find node by node name
     *
     * @param nodeDetails nodeDetails
     * @param nodeName nodeName
     * @return task node
     */
    fun findNodeByName(nodeDetails: List<TaskNode>, nodeName: String?): TaskNode? {
        for (taskNode in nodeDetails) {
            if (taskNode.getName().equals(nodeName)) {
                return taskNode
            }
        }
        return null
    }

    /**
     * find node by node code
     *
     * @param nodeDetails nodeDetails
     * @param nodeCode nodeCode
     * @return task node
     */
    fun findNodeByCode(nodeDetails: List<TaskNode>, nodeCode: String): TaskNode? {
        for (taskNode in nodeDetails) {
            if (taskNode.getCode().toString() == nodeCode) {
                return taskNode
            }
        }
        return null
    }

    /**
     * the task can be submit when all the depends nodes are forbidden or complete
     *
     * @param taskNode taskNode
     * @param dag dag
     * @param completeTaskList completeTaskList
     * @return can submit
     */
    fun allDependsForbiddenOrEnd(taskNode: TaskNode,
                                 dag: DAG<String, TaskNode, TaskNodeRelation>,
                                 skipTaskNodeList: Map<String, TaskNode>,
                                 completeTaskList: Map<String, TaskInstance>): Boolean {
        val dependList: List<String> = taskNode.getDepList() ?: return true
        for (dependNodeCode in dependList) {
            val dependNode: TaskNode? = dag.getNode(dependNodeCode)
            return if (dependNode == null || completeTaskList.containsKey(dependNodeCode)
                    || dependNode.isForbidden()
                    || skipTaskNodeList.containsKey(dependNodeCode)) {
                continue
            } else {
                false
            }
        }
        return true
    }

    /**
     * parse the successor nodes of previous node.
     * this function parse the condition node to find the right branch.
     * also check all the depends nodes forbidden or complete
     *
     * @return successor nodes
     */
    fun parsePostNodes(preNodeCode: String,
                       skipTaskNodeList: MutableMap<String, TaskNode>,
                       dag: DAG<String, TaskNode, TaskNodeRelation>,
                       completeTaskList: Map<String, TaskInstance>): Set<String> {
        val postNodeList: MutableSet<String> = mutableSetOf()
        var startVertexes: MutableSet<String> = mutableSetOf()
        if (preNodeCode == null) {
            startVertexes = dag.getBeginNode().stream().collect(Collectors.toSet())
        } else if (dag.getNode(preNodeCode)?.isConditionsTask()?:false) {
            val conditionTaskList = parseConditionTask(preNodeCode, skipTaskNodeList, dag, completeTaskList)
            startVertexes.addAll(conditionTaskList)
        } else if (dag.getNode(preNodeCode)?.isSwitchTask() ?: false ) {
            val conditionTaskList = parseSwitchTask(preNodeCode, skipTaskNodeList, dag, completeTaskList)
            startVertexes.addAll(conditionTaskList)
        } else {
            startVertexes = dag.getSubsequentNodes(preNodeCode)
        }
        for (subsequent in startVertexes) {
            val taskNode: TaskNode? = dag.getNode(subsequent)
            if (taskNode == null) {
                logger.error("taskNode {} is null, please check dag", subsequent)
                continue
            }
            if (isTaskNodeNeedSkip(taskNode, skipTaskNodeList)) {
                setTaskNodeSkip(subsequent, dag, completeTaskList, skipTaskNodeList)
                continue
            }
            if (!allDependsForbiddenOrEnd(taskNode, dag, skipTaskNodeList, completeTaskList)) {
                continue
            }
            if (taskNode.isForbidden() || completeTaskList.containsKey(subsequent)) {
                postNodeList.addAll(parsePostNodes(subsequent, skipTaskNodeList, dag, completeTaskList))
                continue
            }
            postNodeList.add(subsequent)
        }
        return postNodeList
    }

    /**
     * if all of the task dependence are skipped, skip it too.
     */
    private fun isTaskNodeNeedSkip(taskNode: TaskNode,
                                   skipTaskNodeList: Map<String, TaskNode>): Boolean {
        if (taskNode.getDepList().isEmpty()) {
            return false
        }
        for (depNode in taskNode.getDepList()) {
            if (!skipTaskNodeList.containsKey(depNode)) {
                return false
            }
        }
        return true
    }

    /**
     * parse condition task find the branch process
     * set skip flag for another one.
     */
    fun parseConditionTask(nodeCode: String,
                           skipTaskNodeList: MutableMap<String, TaskNode>,
                           dag: DAG<String, TaskNode, TaskNodeRelation>,
                           completeTaskList: Map<String, TaskInstance>): List<String> {
        var conditionTaskList: MutableList<String> = ArrayList()
        val taskNode: TaskNode? = dag.getNode(nodeCode)
        if (!(taskNode?.isConditionsTask() ?:false)) {
            return conditionTaskList
        }
        if (!completeTaskList.containsKey(nodeCode)) {
            return conditionTaskList
        }
        val taskInstance = completeTaskList[nodeCode]
        if(taskNode != null){
            val conditionsParameters: ConditionsParameters = JSONUtils.parseObject(taskNode.getConditionResult(), ConditionsParameters::class.java)
            var skipNodeList: List<String> = ArrayList()
            if (taskInstance!!.getState().isSuccess()) {
                conditionTaskList = conditionsParameters.getSuccessNode()
                skipNodeList = conditionsParameters.getFailedNode()
            } else if (taskInstance.getState().isFailure()) {
                conditionTaskList = conditionsParameters.getFailedNode()
                skipNodeList = conditionsParameters.getSuccessNode()
            } else {
                conditionTaskList.add(nodeCode)
            }
            // the skipNodeList maybe null if no next task
            skipNodeList = Optional.ofNullable(skipNodeList).orElse(ArrayList())
            for (failedNode in skipNodeList) {
                setTaskNodeSkip(failedNode, dag, completeTaskList, skipTaskNodeList)
            }
        }


        // the conditionTaskList maybe null if no next task
        conditionTaskList = Optional.ofNullable(conditionTaskList).orElse(ArrayList())
        return conditionTaskList
    }

    /**
     * parse condition task find the branch process
     * set skip flag for another one.
     *
     * @param nodeCode
     * @return
     */
    fun parseSwitchTask(nodeCode: String,
                        skipTaskNodeList: MutableMap<String, TaskNode>,
                        dag: DAG<String, TaskNode, TaskNodeRelation>,
                        completeTaskList: Map<String, TaskInstance>): List<String> {
        var conditionTaskList: List<String> = ArrayList()
        val taskNode: TaskNode? = dag.getNode(nodeCode)
        if (!(taskNode?.isSwitchTask()?:false) ) {
            return conditionTaskList
        }
        if (!completeTaskList.containsKey(nodeCode)) {
            return conditionTaskList
        }
        conditionTaskList = skipTaskNode4Switch(taskNode, skipTaskNodeList, completeTaskList, dag)
        return conditionTaskList
    }

    private fun skipTaskNode4Switch(taskNode: TaskNode, skipTaskNodeList: MutableMap<String?, TaskNode?>,
                                    completeTaskList: Map<String?, TaskInstance>,
                                    dag: DAG<String?, TaskNode?, TaskNodeRelation?>): List<String> {
        val switchParameters: SwitchParameters = completeTaskList[taskNode.getCode().toString()].getSwitchDependency()
        val resultConditionLocation: Int = switchParameters.getResultConditionLocation()
        val conditionResultVoList: List<SwitchResultVo> = switchParameters.getDependTaskList()
        var switchTaskList: List<String> = conditionResultVoList[resultConditionLocation].getNextNode()
        if (switchTaskList.isEmpty()) {
            switchTaskList = ArrayList()
        }
        conditionResultVoList.removeAt(resultConditionLocation)
        for (info in conditionResultVoList) {
            if (CollectionUtils.isEmpty(info.getNextNode())) {
                continue
            }
            setTaskNodeSkip(info.getNextNode().get(0), dag, completeTaskList, skipTaskNodeList)
        }
        return switchTaskList
    }

    /**
     * set task node and the post nodes skip flag
     */
    private fun setTaskNodeSkip(skipNodeCode: String,
                                dag: DAG<String, TaskNode, TaskNodeRelation>,
                                completeTaskList: Map<String, TaskInstance>,
                                skipTaskNodeList: MutableMap<String, TaskNode>) {
        if (!dag.containsNode(skipNodeCode)) {
            return
        }
        val node = dag.getNode(skipNodeCode)
        if(node != null){
            skipTaskNodeList.putIfAbsent(skipNodeCode, node)
        }
        val postNodeList: Collection<String> = dag.getSubsequentNodes(skipNodeCode)
        for (post in postNodeList) {
            val postNode: TaskNode? = dag.getNode(post)
            if(postNode != null){
                if (isTaskNodeNeedSkip(postNode, skipTaskNodeList)) {
                    setTaskNodeSkip(post, dag, completeTaskList, skipTaskNodeList)
                }
            }

        }
    }

    /***
     * build dag graph
     * @param processDag processDag
     * @return dag
     */
    fun buildDagGraph(processDag: ProcessDag): DAG<String, TaskNode, TaskNodeRelation> {
        val dag: DAG<String, TaskNode, TaskNodeRelation> = DAG()

        // add vertex
        if (CollectionUtils.isNotEmpty(processDag.getNodes())) {
            for (node in processDag.getNodes()) {
                dag.addNode(node.getCode().toString(), node)
            }
        }

        // add edge
        if (CollectionUtils.isNotEmpty(processDag.getEdges())) {
            for (edge in processDag.getEdges()) {
                dag.addEdge(edge.getStartNode(), edge.getEndNode())
            }
        }
        return dag
    }

    /**
     * get process dag
     *
     * @param taskNodeList task node list
     * @return Process dag
     */
    fun getProcessDag(taskNodeList: List<TaskNode>): ProcessDag {
        val taskNodeRelations: MutableList<TaskNodeRelation> = ArrayList()

        // Traverse node information and build relationships
        for (taskNode in taskNodeList) {
            val preTasks: String = taskNode.getPreTasks()
            val preTasksList: List<String> = JSONUtils.toList(preTasks, String::class.java)

            // If the dependency is not empty
            if (preTasksList != null) {
                for (depNode in preTasksList) {
                    taskNodeRelations.add(TaskNodeRelation(depNode, taskNode.getCode().toString()))
                }
            }
        }
        val processDag = ProcessDag()
        processDag.setEdges(taskNodeRelations)
        processDag.setNodes(taskNodeList)
        return processDag
    }

    /**
     * get process dag
     *
     * @param taskNodeList task node list
     * @return Process dag
     */
    fun getProcessDag(taskNodeList: List<TaskNode>,
                      processTaskRelations: List<ProcessTaskRelation>): ProcessDag {
        val taskNodeMap: MutableMap<Long, TaskNode> = HashMap()
        taskNodeList.forEach(Consumer<TaskNode> { taskNode: TaskNode -> taskNodeMap.putIfAbsent(taskNode.getCode(), taskNode) })
        val taskNodeRelations: MutableList<TaskNodeRelation> = ArrayList()
        for (processTaskRelation in processTaskRelations) {
            val preTaskCode: Long = processTaskRelation.getPreTaskCode()
            val postTaskCode: Long = processTaskRelation.getPostTaskCode()
            if (processTaskRelation.getPreTaskCode() !== 0 && taskNodeMap.containsKey(preTaskCode) && taskNodeMap.containsKey(postTaskCode)) {
                val preNode = taskNodeMap[preTaskCode]
                val postNode = taskNodeMap[postTaskCode]
                taskNodeRelations
                        .add(TaskNodeRelation(preNode.getCode().toString(), postNode.getCode().toString()))
            }
        }
        val processDag = ProcessDag()
        processDag.setEdges(taskNodeRelations)
        processDag.setNodes(taskNodeList)
        return processDag
    }

    /**
     * is there have conditions after the parent node
     */
    fun haveConditionsAfterNode(parentNodeCode: String?,
                                dag: DAG<String?, TaskNode?, TaskNodeRelation?>): Boolean {
        return haveSubAfterNode(parentNodeCode, dag, TaskConstants.TASK_TYPE_CONDITIONS)
    }

    /**
     * is there have conditions after the parent node
     */
    fun haveConditionsAfterNode(parentNodeCode: String, taskNodes: List<TaskNode>): Boolean {
        if (CollectionUtils.isEmpty(taskNodes)) {
            return false
        }
        for (taskNode in taskNodes) {
            val preTasksList: List<String> = JSONUtils.toList(taskNode.getPreTasks(), String::class.java)
            if (preTasksList.contains(parentNodeCode) && taskNode.isConditionsTask()) {
                return true
            }
        }
        return false
    }

    /**
     * is there have blocking node after the parent node
     */
    fun haveBlockingAfterNode(parentNodeCode: String?,
                              dag: DAG<String?, TaskNode?, TaskNodeRelation?>): Boolean {
        return haveSubAfterNode(parentNodeCode, dag, TaskConstants.TASK_TYPE_BLOCKING)
    }

    /**
     * is there have all node after the parent node
     */
    fun haveAllNodeAfterNode(parentNodeCode: String?,
                             dag: DAG<String?, TaskNode?, TaskNodeRelation?>): Boolean {
        return haveSubAfterNode(parentNodeCode, dag, null)
    }

    /**
     * Whether there is a specified type of child node after the parent node
     */
    fun haveSubAfterNode(parentNodeCode: String?,
                         dag: DAG<String?, TaskNode?, TaskNodeRelation?>, filterNodeType: String?): Boolean {
        val subsequentNodes: Set<String> = dag.getSubsequentNodes(parentNodeCode)
        if (CollectionUtils.isEmpty(subsequentNodes)) {
            return false
        }
        if (org.apache.commons.lang3.StringUtils.isBlank(filterNodeType)) {
            return true
        }
        for (nodeName in subsequentNodes) {
            val taskNode: TaskNode = dag.getNode(nodeName)
            if (taskNode.getType().equalsIgnoreCase(filterNodeType)) {
                return true
            }
        }
        return false
    }
}
