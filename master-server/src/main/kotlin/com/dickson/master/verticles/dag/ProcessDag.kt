package com.dickson.master.verticles.dag

import com.dickson.master.verticles.TaskNode
import com.dickson.master.verticles.TaskNodeRelation


class ProcessDag {
    /**
     * DAG edge list
     */
    private var edges: List<TaskNodeRelation> = listOf()

    /**
     * DAG node list
     */
    private var nodes: List<TaskNode> = listOf()

    /**
     * getter method
     *
     * @return the edges
     * @see ProcessDag.edges
     */
    fun getEdges(): List<TaskNodeRelation> {
        return edges
    }

    /**
     * setter method
     *
     * @param edges the edges to set
     * @see ProcessDag.edges
     */
    fun setEdges(edges: List<TaskNodeRelation>) {
        this.edges = edges
    }

    /**
     * getter method
     *
     * @return the nodes
     * @see ProcessDag.nodes
     */
    fun getNodes(): List<TaskNode> {
        return nodes
    }

    /**
     * setter method
     *
     * @param nodes the nodes to set
     * @see ProcessDag.nodes
     */
    fun setNodes(nodes: List<TaskNode>) {
        this.nodes = nodes
    }

    override fun toString(): String {
        return ("ProcessDag{"
                + "edges=" + edges
                + ", nodes=" + nodes
                + '}')
    }
}

