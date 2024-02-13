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

import io.smallrye.mutiny.Uni
import io.vertx.core.Promise
import kotlinx.coroutines.delay
import org.apache.camel.CamelContext
import org.apache.camel.ProducerTemplate
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.ws.rs.GET
import javax.ws.rs.Path
import kotlin.system.measureTimeMillis

@Path("hello")
class MyResource(
    val producerTemplate: ProducerTemplate,
    val camelContext: CamelContext,

) {


     val logger: Logger = LoggerFactory.getLogger(MyResource::class.java)

    @GET
    @Path("b")
    suspend fun run(): String {
         return measureTimeMillis {
             val value = producerTemplate.asyncRequestBody("direct:b", "hello", String::class.java)
             delay(1000)
             logger.info("Active count {}, Free count {}",ThreadPoolManager.THREADPOOL.activeCount, ThreadPoolManager.THREADPOOL.queue - ThreadPoolManager.THREADPOOL.activeCount)
             value.await()
             logger.info("Active count {}, Free count {}",ThreadPoolManager.THREADPOOL.activeCount, ThreadPoolManager.THREADPOOL.poolSize - ThreadPoolManager.THREADPOOL.queue.size)

         }.toString()
    }

    @GET
    @Path("c")
    suspend fun run2():String{
        return measureTimeMillis { println("a") }.toString()
    }

    @GET
    @Path("d")
    suspend fun run3():String{
//        return
        return measureTimeMillis {
        camelContext.createFluentProducerTemplate().withProcessor { ex ->
            ex.properties.put("promise", Promise.promise<String>());
            ex.getIn().body = "hello"
        }.to("direct:b")
            .asyncRequest(String::class.java).await()
        }.toString()
    }




}