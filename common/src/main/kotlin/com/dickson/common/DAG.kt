/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */
package com.dickson.common

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

/**
 * analysis of DAG
 * Node: node
 * NodeInfo: node description information
 * EdgeInfo: edge description information
 */
open class DAG<Node, NodeInfo, EdgeInfo> {

    /**
     * node map, key is node, value is node information
     */
    private val nodesMap: MutableMap<Node, NodeInfo?>

    /**
     * edge map. key is node of origin;value is Map with key for destination node and value for edge
     */
    private val edgesMap: MutableMap<Node, MutableMap<Node, EdgeInfo?>>

    /**
     * reversed edge set, key is node of destination, value is Map with key for origin node and value for edge
     */
    private val reverseEdgesMap: MutableMap<Node, MutableMap<Node, EdgeInfo?>>

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(DAG::class.java)
    }

    init {
        nodesMap = HashMap()
        edgesMap = HashMap()
        reverseEdgesMap = HashMap()
    }

    /**
     * add node information
     *
     * @param node node
     * @param nodeInfo node information
     */
    open fun addNode(node: Node, nodeInfo: NodeInfo?) {
        nodesMap[node] = nodeInfo
    }

    /**
     * add edge
     *
     * @param fromNode node of origin
     * @param toNode node of destination
     * @return The result of adding an edge. returns false if the DAG result is a ring result
     */
    open fun addEdge(fromNode: Node, toNode: Node) = addEdge(fromNode, toNode, false)

    /**
     * add edge
     *
     * @param fromNode node of origin
     * @param toNode node of destination
     * @param createNode whether the node needs to be created if it does not exist
     * @return The result of adding an edge. returns false if the DAG result is a ring result
     */
    private fun addEdge(fromNode: Node, toNode: Node, createNode: Boolean) = addEdge(fromNode, toNode, null, createNode)

    /**
     * add edge
     *
     * @param fromNode node of origin
     * @param toNode node of destination
     * @param edge edge description
     * @param createNode whether the node needs to be created if it does not exist
     * @return The result of adding an edge. returns false if the DAG result is a ring result
     */
    open fun addEdge(fromNode: Node, toNode: Node, edge: EdgeInfo?, createNode: Boolean): Boolean {
        // Whether an edge can be successfully added(fromNode -> toNode)
        if (!isLegalAddEdge(fromNode, toNode, createNode)) {
            logger.error(
                    "serious error: add edge({} -> {}) is invalid, cause cycle!",
                    fromNode,
                    toNode
            )
            return false
        }
        addNodeIfAbsent(fromNode, null)
        addNodeIfAbsent(toNode, null)
        addEdge(fromNode, toNode, edge, edgesMap)
        addEdge(toNode, fromNode, edge, reverseEdgesMap)
        return true
    }

    /**
     * whether this node is contained
     *
     * @param node node
     * @return true if contains
     */
    open fun containsNode(node: Node) = nodesMap.containsKey(node)

    /**
     * whether this edge is contained
     *
     * @param fromNode node of origin
     * @param toNode node of destination
     * @return true if contains
     */
    open fun containsEdge(fromNode: Node, toNode: Node): Boolean {
        val endEdges = edgesMap[fromNode] ?: return false
        return endEdges.containsKey(toNode)
    }

    /**
     * get node description
     *
     * @param node node
     * @return node description
     */
    open fun getNode(node: Node) = nodesMap[node]

    /**
     * Get the number of nodes
     *
     * @return the number of nodes
     */
    open val nodesCount: Int
        get() {
            return nodesMap.size
        }

    /**
     * Get the number of edges
     *
     * @return the number of edges
     */
    open val edgesCount: Int
        get() {
            var count = 0
            for ((_, value) in edgesMap) {
                count += value.size
            }
            return count
        }

    /**
     * get the start node of DAG
     *
     * @return the start node of DAG
     */
    open val beginNode: Collection<Node>
        get() = nodesMap.keys.subtract(reverseEdgesMap.keys)

    /**
     * get the end node of DAG
     *
     * @return the end node of DAG
     */
    open val endNode: Collection<Node>
        get() = nodesMap.keys.subtract(edgesMap.keys)

    /**
     * Gets all previous nodes of the node
     *
     * @param node node id to be calculated
     * @return all previous nodes of the node
     */
    open fun getPreviousNodes(node: Node) = getNeighborNodes(node, reverseEdgesMap)

    /**
     * Get all subsequent nodes of the node
     *
     * @param node node id to be calculated
     * @return all subsequent nodes of the node
     */
    open fun getSubsequentNodes(node: Node) = getNeighborNodes(node, edgesMap)

    /**
     * Gets the degree of entry of the node
     *
     * @param node node id
     * @return the degree of entry of the node
     */
    open fun getIndegree(node: Node) = getPreviousNodes(node).size

    /**
     * whether the graph has a ring
     *
     * @return true if has cycle, else return false.
     */
    open fun hasCycle() = !topologicalSortImpl().key

    /**
     * Only DAG has a topological sort
     *
     * @return topologically sorted results, returns false if the DAG result is a ring result
     * @throws Exception errors
     */
    @Throws(Exception::class)
    open fun topologicalSort(): List<Node> {
        val entry = topologicalSortImpl()
        if (entry.key) {
            return entry.value
        }
        throw Exception("serious error: graph has cycle ! ")
    }

    /**
     * if tho node does not exist,add this node
     *
     * @param node node
     * @param nodeInfo node information
     */
    private fun addNodeIfAbsent(node: Node, nodeInfo: NodeInfo?) {
        if (!containsNode(node)) {
            addNode(node, nodeInfo)
        }
    }

    /**
     * add edge
     *
     * @param fromNode node of origin
     * @param toNode node of destination
     * @param edge edge description
     * @param edges edge set
     */
    private fun addEdge(
            fromNode: Node,
            toNode: Node,
            edge: EdgeInfo?,
            edges: MutableMap<Node, MutableMap<Node, EdgeInfo?>>
    ) {
        edges.putIfAbsent(fromNode, HashMap())
        val toNodeEdges = edges[fromNode]!!
        toNodeEdges[toNode] = edge
    }

    /**
     * Whether an edge can be successfully added(fromNode -> toNode)
     * need to determine whether the DAG has cycle
     *
     * @param fromNode node of origin
     * @param toNode node of destination
     * @param createNode whether to create a node
     * @return true if added
     */
    private fun isLegalAddEdge(fromNode: Node, toNode: Node, createNode: Boolean): Boolean {
        if (fromNode == toNode) {
            logger.error(
                    "edge fromNode({}) can't equals toNode({})",
                    fromNode,
                    toNode
            )
            return false
        }
        if (!createNode) {
            if (!containsNode(fromNode) || !containsNode(toNode)) {
                logger.error(
                        "edge fromNode({}) or toNode({}) is not in vertices map",
                        fromNode,
                        toNode
                )
                return false
            }
        }

        // Whether an edge can be successfully added(fromNode -> toNode),need to determine whether the DAG has cycle!
        var verticesCount = nodesCount
        val queue: Queue<Node> = LinkedList()
        queue.add(toNode)

        // if DAG doesn't find fromNode, it's not has cycle!
        while (!queue.isEmpty() && --verticesCount > 0) {
            val key = queue.poll()
            for (subsequentNode in getSubsequentNodes(key)) {
                if (subsequentNode == fromNode) {
                    return false
                }
                queue.add(subsequentNode)
            }
        }
        return true
    }

    /**
     * Get all neighbor nodes of the node
     *
     * @param node Node id to be calculated
     * @param edges neighbor edge information
     * @return all neighbor nodes of the node
     */
    private fun getNeighborNodes(node: Node, edges: Map<Node, MutableMap<Node, EdgeInfo?>>): MutableSet<Node> {
        val neighborEdges = edges[node] ?: return mutableSetOf()
        return neighborEdges.keys
    }

    /**
     * get the start node of DAG
     *
     * @return the start node of DAG
     */
    fun getBeginNode(): Collection<Node> {
        return nodesMap.keys.subtract(reverseEdgesMap.keys)
    }

    /**
     * Determine whether there are ring and topological sorting results
     *
     *
     * Directed acyclic graph (DAG) has topological ordering
     * Breadth First Search:
     * 1. Traversal of all the vertices in the graph, the degree of entry is 0 vertex into the queue
     * 2. Poll a vertex in the queue to update its adjacency (minus 1) and queue the adjacency if it is 0 after minus 1
     * 3. Do step 2 until the queue is empty
     * If you cannot traverse all the nodes, it means that the current graph is not a directed acyclic graph.
     * There is no topological sort.
     *
     * @return key Returns the state
     * if success (acyclic) is true, failure (acyclic) is looped,
     * and value (possibly one of the topological sort results)
     */
    private fun topologicalSortImpl(): Map.Entry<Boolean, List<Node>> {
        // node queue with degree of entry 0
        val zeroIndegreeNodeQueue: Queue<Node> = LinkedList()
        // save result
        val topoResultList: MutableList<Node> = ArrayList()
        // save the node whose degree is not 0
        val notZeroIndegreeNodeMap: MutableMap<Node, Int> = HashMap()

        // Scan all the vertices and push vertexs with an entry degree of 0 to queue
        for ((node) in nodesMap) {
            val inDegree = getIndegree(node)
            if (inDegree == 0) {
                zeroIndegreeNodeQueue.add(node)
                topoResultList.add(node)
            } else {
                notZeroIndegreeNodeMap[node] = inDegree
            }
        }

        /*
         * After scanning, there is no node with 0 degree of entry,
         * indicating that there is a ring, and return directly
         */if (zeroIndegreeNodeQueue.isEmpty()) {
            return AbstractMap.SimpleEntry<Boolean, List<Node>>(false, topoResultList)
        }

        // The topology algorithm is used to delete nodes with 0 degree of entry and its associated edges
        while (!zeroIndegreeNodeQueue.isEmpty()) {
            val v = zeroIndegreeNodeQueue.poll()
            // Get the neighbor node
            val subsequentNodes = getSubsequentNodes(v)
            for (subsequentNode in subsequentNodes) {
                val degree: Int? = notZeroIndegreeNodeMap[subsequentNode]
                var workingDegree = degree!!
                if (--workingDegree == 0) {
                    topoResultList.add(subsequentNode)
                    zeroIndegreeNodeQueue.add(subsequentNode)
                    notZeroIndegreeNodeMap.remove(subsequentNode)
                } else {
                    notZeroIndegreeNodeMap[subsequentNode] = workingDegree
                }
            }
        }
        // if notZeroIndegreeNodeMap is empty,there is no ring!
        return AbstractMap.SimpleEntry<Boolean, List<Node>>(notZeroIndegreeNodeMap.isEmpty(), topoResultList)
    }

    override fun toString() = "DAG{nodesMap=$nodesMap, edgesMap=$edgesMap, reverseEdgesMap=$reverseEdgesMap}"

    fun threadSafe(): DAG<Node, NodeInfo, EdgeInfo> = object : DAG<Node, NodeInfo, EdgeInfo>() {
        private val lock: ReadWriteLock = ReentrantReadWriteLock()

        override fun addNode(node: Node, nodeInfo: NodeInfo?) {
            this.lock.writeLock().withLock {
                this@DAG.addNode(node, nodeInfo)
            }
        }

        override fun addEdge(fromNode: Node, toNode: Node): Boolean {
            return this.lock.writeLock().withLock {
                this@DAG.addEdge(fromNode, toNode, false)
            }
        }


        override fun addEdge(fromNode: Node, toNode: Node, edge: EdgeInfo?, createNode: Boolean) =
                this.lock.writeLock().withLock {
                    this@DAG.addEdge(fromNode, toNode, edge, createNode)
                }


        override fun containsNode(node: Node) = lock.readLock().withLock {
            this@DAG.containsNode(node)
        }

        override fun containsEdge(fromNode: Node, toNode: Node) = lock.readLock().withLock {
            this@DAG.containsEdge(fromNode, toNode)
        }

        override fun getNode(node: Node) = lock.readLock().withLock {
            this@DAG.getNode(node)
        }

        override val nodesCount: Int
            get() = this.lock.readLock().withLock { this@DAG.nodesCount }

        override val edgesCount: Int
            get() = lock.readLock().withLock { this@DAG.edgesCount }

        /**
         * get the start node of DAG
         *
         * @return the start node of DAG
         */
        override val beginNode: Collection<Node>
            get() = lock.readLock().withLock {
                this@DAG.beginNode
            }

        override val endNode: Collection<Node>
            get() = lock.readLock().withLock { this@DAG.endNode }

        override fun getPreviousNodes(node: Node) = lock.readLock().withLock {
            this@DAG.getPreviousNodes(node)
        }

        override fun getSubsequentNodes(node: Node) = lock.readLock().withLock {
            this@DAG.getSubsequentNodes(node)
        }

        override fun getIndegree(node: Node) = lock.readLock().withLock { this@DAG.getIndegree(node) }

        override fun hasCycle() = lock.readLock().withLock { this@DAG.hasCycle() }

        @Throws(Exception::class)
        override fun topologicalSort() = lock.readLock().withLock {
            this@DAG.topologicalSort()
        }
    }



}