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
package com.dickson.common.enums

import java.util.*
import java.util.function.Function
import java.util.stream.Collectors

/**
 * data base types
 */
enum class ResUploadType {
    /**
     * 0 hdfs
     * 1 s3
     * 2 none
     */
    HDFS,
    S3,
    NONE
}

/**
* resource type
*/
enum class ResourceType(//@EnumValue
    val code: Int, val descp: String
) {
    /**
     * 0 file, 1 udf
     */
    FILE(0, "file"),
    UDF(1, "udf");
}

/**
 * have_script
 * have_file
 * can_retry
 * have_arr_variables
 * have_map_variables
 * have_alert
 */
enum class Flag(val code: Int, val descp: String) {
    /**
     * 0 no
     * 1 yes
     */
    NO(0, "no"),
    YES(1, "yes");

}


enum class DbType(//@EnumValue
    val code: Int, val descp: String
) {
    MYSQL(0, "mysql"),
    POSTGRESQL(1, "postgresql"),
    HIVE(2, "hive"),
    SPARK(3, "spark"),
    CLICKHOUSE(4,"clickhouse"),
    ORACLE(5, "oracle"),
    SQLSERVER(6, "sqlserver"),
    DB2(7, "db2"),
    PRESTO(8, "presto"),
    H2(9, "h2"),
    REDSHIFT(10,"redshift");

    val isHive: Boolean
        get() = this == HIVE

    companion object {
        private val DB_TYPE_MAP = Arrays.stream(values()).collect(
            Collectors.toMap(
            { obj: DbType -> obj.code }, Function.identity()
        )
        )

        fun of(type: Int): DbType? {
            return if (DB_TYPE_MAP.containsKey(type)) {
                DB_TYPE_MAP[type]
            } else null
        }

        fun ofName(name: String): DbType {
            return Arrays.stream(values()).filter { e: DbType -> e.name == name }
                .findFirst().orElseThrow { NoSuchElementException("no such db type") }
        }
    }
}


enum class DbConnectType(val code: Int, val descp: String) {
    ORACLE_SERVICE_NAME(0, "Oracle Service Name"),
    ORACLE_SID(1, "Oracle SID");

}

/**
 * command types
 */
enum class CommandType(val code: Int, val descp: String) {
    /**
     * command types
     * 0 start a new process
     * 1 start a new process from current nodes
     * 2 recover tolerance fault process
     * 3 recover suspended process
     * 4 start process from failure task nodes
     * 5 complement data
     * 6 start a new process from scheduler
     * 7 repeat running a process
     * 8 pause a process
     * 9 stop a process
     * 10 recover waiting thread
     */
    START_PROCESS(0, "start a new process"),
    START_CURRENT_TASK_PROCESS(1, "start a new process from current nodes"),
    RECOVER_TOLERANCE_FAULT_PROCESS(2, "recover tolerance fault process"),
    RECOVER_SUSPENDED_PROCESS(3, "recover suspended process"),
    START_FAILURE_TASK_PROCESS(4, "start process from failure task nodes"),
    COMPLEMENT_DATA(5, "complement data"),
    SCHEDULER(6, "start a new process from scheduler"),
    REPEAT_RUNNING(7, "repeat running a process"),
    PAUSE(8, "pause a process"),
    STOP(9, "stop a process"),
    RECOVER_WAITING_THREAD(10, "recover waiting thread"),
    RECOVER_SERIAL_WAIT(11, "recover serial wait");

    companion object {
        private val COMMAND_TYPE_MAP: MutableMap<Int, CommandType> = HashMap()
        @JvmStatic
        fun of(status: Int): CommandType? {
            if (COMMAND_TYPE_MAP.containsKey(status)) {
                return COMMAND_TYPE_MAP[status]
            }
            throw IllegalArgumentException("invalid status : $status")
        }

        init {
            for (commandType in values()) {
                COMMAND_TYPE_MAP[commandType.code] = commandType
            }
        }
    }
}