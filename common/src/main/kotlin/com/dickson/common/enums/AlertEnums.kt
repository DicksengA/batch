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

enum class AlertEvent {
    SERVER_DOWN, TIME_OUT
}

/**
 * alert sending(execution) status
 */
enum class AlertStatus(val code: Int, val descp: String) {
    /**
     * 0 waiting executed; 1 execute successfully，2 execute failed
     */
    WAIT_EXECUTION(0, "waiting executed"), EXECUTION_SUCCESS(1, "execute successfully"), EXECUTION_FAILURE(
        2,
        "execute failed"
    ),
    EXECUTION_PARTIAL_SUCCESS(3, "execute partial successfully");

}

/**
 * describe the reason why alert generates
 */
enum class AlertType(val code: Int, val descp: String) {
    /**
     * 0 process instance failure, 1 process instance success, 2 process instance blocked, 3 process instance timeout, 4 fault tolerance warning,
     * 5 task failure, 6 task success, 7 task timeout, 8 close alert
     */
    PROCESS_INSTANCE_FAILURE(0, "process instance failure"), PROCESS_INSTANCE_SUCCESS(
        1,
        "process instance success"
    ),
    PROCESS_INSTANCE_BLOCKED(2, "process instance blocked"), PROCESS_INSTANCE_TIMEOUT(
        3,
        "process instance timeout"
    ),
    FAULT_TOLERANCE_WARNING(4, "fault tolerance warning"), TASK_FAILURE(5, "task failure"), TASK_SUCCESS(
        6,
        "task success"
    ),
    TASK_TIMEOUT(7, "task timeout"), CLOSE_ALERT(8, "the process instance success, can close the before alert");

}

enum class AlertWarnLevel {
    MIDDLE, SERIOUS
}
