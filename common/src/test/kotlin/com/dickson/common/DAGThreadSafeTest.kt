package com.dickson.common

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.*

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


class DAGThreadSafeTest {
    private var graph: DAG<Int, String, String>? = null
    @BeforeEach
    fun setup() {
        graph = DAG<Int, String, String>().threadSafe()
    }

    @AfterEach
    fun tearDown() {
        clear()
    }

    private fun clear() {
        graph = null
        graph = DAG<Int, String, String>().threadSafe()
        assertEquals(graph?.nodesCount, 0)
    }

    private fun makeGraph() {
        clear()

        //         1->2
        //         2->5
        //         3->5
        //         4->6
        //         5->6
        //         6->7
        for (i in 1..7) {
            graph!!.addNode(i, "v($i)")
        }

        // construction side
        assertTrue(graph!!.addEdge(1, 2))
        assertTrue(graph!!.addEdge(2, 5))
        assertTrue(graph!!.addEdge(3, 5))
        assertTrue(graph!!.addEdge(4, 6))
        assertTrue(graph!!.addEdge(5, 6))
        assertTrue(graph!!.addEdge(6, 7))
        assertEquals(graph?.nodesCount, 7)
        assertEquals(graph?.edgesCount, 6)
    }

    /**
     * add node
     */
    @Test
    fun testAddNode() {
        clear()
        graph!!.addNode(1, "v(1)")
        graph!!.addNode(2, null)
        graph!!.addNode(5, "v(5)")
        assertEquals(graph?.nodesCount, 3)
        assertEquals(graph!!.getNode(1), "v(1)")
        assertTrue(graph!!.containsNode(1))
        assertFalse(graph!!.containsNode(10))
    }

    /**
     * add edge
     */
    @Test
    fun testAddEdge() {
        clear()
        assertFalse(graph!!.addEdge(1, 2, "edge(1 -> 2)", false))
        graph!!.addNode(1, "v(1)")
        assertTrue(graph!!.addEdge(1, 2, "edge(1 -> 2)", true))
        graph!!.addNode(2, "v(2)")
        assertTrue(graph!!.addEdge(1, 2, "edge(1 -> 2)", true))
        assertFalse(graph!!.containsEdge(1, 3))
        assertTrue(graph!!.containsEdge(1, 2))
        assertEquals(graph?.edgesCount, 1)
        val node = 3
        graph!!.addNode(node, "v(3)")
        assertFalse(graph!!.addEdge(node, node))
    }

    /**
     * add subsequent node
     */
    @Test
    fun testSubsequentNodes() {
        makeGraph()
        assertEquals(graph!!.getSubsequentNodes(1).size, 1)
    }

    /**
     * test indegree
     */
    @Test
    fun testIndegree() {
        makeGraph()
        assertEquals(graph!!.getIndegree(1), 0)
        assertEquals(graph!!.getIndegree(2), 1)
        assertEquals(graph!!.getIndegree(3), 0)
        assertEquals(graph!!.getIndegree(4), 0)
    }

    /**
     * test begin node
     */
    @Test
    fun testBeginNode() {
        makeGraph()
        assertEquals(graph?.beginNode?.size, 3)
        assertTrue(graph!!.beginNode.contains(1))
        assertTrue(graph!!.beginNode.contains(3))
        assertTrue(graph!!.beginNode.contains(4))
    }

    /**
     * test end node
     */
    @Test
    fun testEndNode() {
        makeGraph()
        assertEquals(graph!!.endNode.size, 1)
        assertTrue(graph!!.endNode.contains(7))
    }

    /**
     * test cycle
     */
    @Test
    fun testCycle() {
        clear()
        for (i in 1..5) {
            graph!!.addNode(i, "v($i)")
        }

        // construction side
        try {
            graph!!.addEdge(1, 2)
            graph!!.addEdge(2, 3)
            graph!!.addEdge(3, 4)
            assertFalse(graph!!.hasCycle())
        } catch (e: Exception) {
            e.printStackTrace()
            fail()
        }
        try {
            val addResult = graph!!.addEdge(4, 1)
            if (!addResult) {
                assertTrue(true)
            }
            graph!!.addEdge(5, 1)
            assertFalse(graph!!.hasCycle())
        } catch (e: Exception) {
            e.printStackTrace()
            fail()
        }
        clear()

        // construction node
        for (i in 1..5) {
            graph!!.addNode(i, "v($i)")
        }

        // construction side, 1->2, 2->3, 3->4
        try {
            graph!!.addEdge(1, 2)
            graph!!.addEdge(2, 3)
            graph!!.addEdge(3, 4)
            graph!!.addEdge(4, 5)
            graph!!.addEdge(5, 2) //?????????????????
            assertFalse(graph!!.hasCycle())
        } catch (e: Exception) {
            e.printStackTrace()
            fail()
        }
    }

    @Test
    fun testTopologicalSort() {
        makeGraph()
        try {
            // topological result is : 1 3 4 2 5 6 7
            val topoList: MutableList<Int> = ArrayList()
            topoList.add(1)
            topoList.add(3)
            topoList.add(4)
            topoList.add(2)
            topoList.add(5)
            topoList.add(6)
            topoList.add(7)
            assertEquals(graph!!.topologicalSort(), topoList)
        } catch (e: Exception) {
            e.printStackTrace()
            fail()
        }
    }

    @Test
    fun testTopologicalSort2() {
        clear()
        graph!!.addEdge(1, 2, null, true)
        graph!!.addEdge(2, 3, null, true)
        graph!!.addEdge(3, 4, null, true)
        graph!!.addEdge(4, 5, null, true)
        graph!!.addEdge(5, 1, null, false) //The loop will fail to add
        try {
            val topoList: MutableList<Int> = ArrayList() // topological result is : 1 2 3 4 5
            topoList.add(1)
            topoList.add(2)
            topoList.add(3)
            topoList.add(4)
            topoList.add(5)
            assertEquals(graph!!.topologicalSort(), topoList)
        } catch (e: Exception) {
            e.printStackTrace()
            fail()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTopologicalSort3() {
        clear()

        //         1->2
        //         1->3
        //         2->5
        //         3->4
        //         4->6
        //         5->6
        //         6->7
        //         6->8
        for (i in 1..8) {
            graph!!.addNode(i, "v($i)")
        }

        // construction node
        assertTrue(graph!!.addEdge(1, 2))
        assertTrue(graph!!.addEdge(1, 3))
        assertTrue(graph!!.addEdge(2, 5))
        assertTrue(graph!!.addEdge(3, 4))
        assertTrue(graph!!.addEdge(4, 6))
        assertTrue(graph!!.addEdge(5, 6))
        assertTrue(graph!!.addEdge(6, 7))
        assertTrue(graph!!.addEdge(6, 8))
        assertEquals(graph!!.nodesCount, 8)
        logger.info(Arrays.toString(graph!!.topologicalSort().toIntArray()))
        val expectedList: MutableList<Int> = ArrayList()
        for (i in 1..8) {
            expectedList.add(i)
            logger.info(i.toString() + " subsequentNodes : " + graph!!.getSubsequentNodes(i))
        }
        logger.info(6.toString() + "  previousNodesb: " + graph!!.getPreviousNodes(6))
        assertEquals(5, graph!!.getSubsequentNodes(2).toIntArray().get(0))
    }

    @Test
    fun testTopologicalSort4() {
        clear()
        try {
            graph!!.topologicalSort()
        } catch (e: Exception) {
            assertTrue(e.message!!.contains("serious error: graph has cycle"))
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DAGThreadSafeTest::class.java)
    }
}