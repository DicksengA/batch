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

import io.smallrye.mutiny.coroutines.awaitSuspending
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import org.apache.camel.CamelContext
import org.apache.camel.Exchange
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.spi.Synchronization
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


object AMain {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>): Unit {
        runBlocking {
            val vertx = Vertx.vertx()
            val camelContext: DefaultCamelContext = object: DefaultCamelContext(){
                override fun isUseMDCLogging(): Boolean {
                    return true;
                }
            }
            System.out.println("MDC " + camelContext.isUseMDCLogging)
            val stringFuture = vertx.deployVerticle(MyVerticle(camelContext))
            val s = stringFuture.toCompletionStage().toCompletableFuture().get()
            val newFixedThreadPool = Executors.newFixedThreadPool(10);


            val launch = CoroutineScope(newFixedThreadPool.asCoroutineDispatcher()).launch {
                val job = launch {

                    val fluentProducerTemplate = camelContext.createFluentProducerTemplate()
                    //        CountDownLatch latch = new CountDownLatch(2);
                    val hello = fluentProducerTemplate.to("direct:a").withBody("hello").asyncRequest()
//                   val vaule:String =  hello.getResult()
                    println(hello.await())
                }
                val job1 = launch {
                    val hello =
                        camelContext.createFluentProducerTemplate().to("direct:a").withBody("hello").asyncRequest(
                            String::class.java
                        )
                    System.out.println(hello.get());
                }
                joinAll(job, job1)
//                job.join()

//
            }
            launch.join()
            System.out.println("Completed")
            vertx.undeploy(s).toCompletionStage().await()
            System.out.println("Undeployed")
            Runtime.getRuntime().addShutdownHook(Thread(Runnable { }))

            vertx.close {
                newFixedThreadPool.shutdown()
            }
//
        }


    }


}


class MyVerticle(var camelContext: CamelContext) : CoroutineVerticle() {
    val threadPoolExecutor = ThreadPoolExecutor(
        1, 1, 0, TimeUnit.MILLISECONDS,
        PriorityBlockingQueue(),
        ThreadPoolExecutor.CallerRunsPolicy()
    )
    override suspend fun start() {


        val logger = LoggerFactory.getLogger("hello")
        camelContext.addRoutes(object : RouteBuilder() {
            @Throws(Exception::class)
            override fun configure() {
                from("direct:a")
                    .threads().executorService(threadPoolExecutor)
                    .process { exchange ->

                        val message = exchange.message
                        val body = message.body
                        message.body = "why $body"
                        logger.info("why $body")
                    }
            }
        })
        camelContext.start()
        launch {
            while (!camelContext.isStarted) {
                delay(1000)
            }
        }.join()
    }

    @Throws(Exception::class)
    override suspend fun stop() {
        camelContext.stop()
        threadPoolExecutor.shutdown();
        val launch = launch {
            while (camelContext.isStopped != true) {
                delay(1000)
            }

        }
        launch.join()

        System.out.println("Stopped")
    }
}


suspend fun <V> java.util.concurrent.Future<V>.await(): V {
    while (!this.isDone) {
        delay(1000)
    }
    return this.get()
}


suspend fun <V> Exchange.getResult(): V {
    val unitOfWork = this.unitOfWork;
    val promise = io.vertx.mutiny.core.Promise.promise<V>()
    val syn = object : Synchronization {
        override fun onComplete(exchange: Exchange) {
            unitOfWork.removeSynchronization(this)
            promise.complete(exchange.message.body as V)
        }

        override fun onFailure(exchange: Exchange) {

            unitOfWork.removeSynchronization(this)
            promise.fail(exchange.exception)
        }

    }
    unitOfWork.addSynchronization(syn)
    return promise.future().awaitSuspending()

}
