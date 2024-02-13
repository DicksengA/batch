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

package com.dickson

import io.vertx.core.Promise
import org.apache.camel.ExtendedCamelContext
import org.apache.camel.builder.RouteBuilder
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
class MyRoutes : RouteBuilder() {
    val logger = LoggerFactory.getLogger(MyRoutes::class.java)

    override fun configure() {
        context.getExtension(ExtendedCamelContext::class.java).setUnitOfWorkFactory(CustomUnitOfWorkFactory())
        from("direct:b")
            .threads().executorService(ThreadPoolManager.THREADPOOL)
            .process { exchange ->
                Thread.sleep(5000)
                MDC.put("id", "wherer1234")
                logger.info("Started processing")
                val property = exchange.getProperty("promise", Promise::class.java)
                logger.info("property is null + {}", property == null)
                val message = exchange.message
                val body = message.body
                message.body = "why $body"

            }

        logger.info("Configured")

    }


}