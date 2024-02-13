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

package org.acme

import io.grpc.MethodDescriptor
import io.quarkus.example.GreeterGrpc
import io.quarkus.example.HelloReply
import io.quarkus.example.HelloRequest
import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.operators.multi.builders.EmitterBasedMulti
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor
import io.vertx.core.Vertx
import io.vertx.core.net.SocketAddress
import io.vertx.grpc.client.GrpcClient
import io.vertx.grpc.client.GrpcClientRequest
import io.vertx.grpc.client.GrpcClientResponse
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ActorScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.flow.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean


@QuarkusTest
class ATest {
//   // @GrpcClient
//    lateinit var hello: Greeter
//
//
//    @Test
//    fun b(){
//        val indefinitely = hello.sayHello(HelloRequest.newBuilder().setName("name").build())
//            .onItem().transform { helloReply: HelloReply -> helloReply.message }
//            .await().indefinitely()
//
//        System.out.println(indefinitely)
//    }

    @Test
    fun c(): Unit {
        val client = GrpcClient.client(Vertx.vertx());
        val server: SocketAddress = SocketAddress.inetSocketAddress(10030, "localhost")
        val sayHelloMethod: MethodDescriptor<HelloRequest, HelloReply> = GreeterGrpc.getSayHelloMethod()
        client.request(server, GreeterGrpc.getSayHelloMethod())
            .compose { request: GrpcClientRequest<HelloRequest, HelloReply> ->
                request.end(
                    HelloRequest
                        .newBuilder()
                        .setName("Bob")
                        .build()
                )
                request.response()
                    .compose { response: GrpcClientResponse<HelloRequest, HelloReply> -> response.last() }
            }.onSuccess { reply: HelloReply -> println("Received " + reply.message) }
            .onFailure { it.printStackTrace() }

        Thread.sleep(2000)
        System.out.println("indefinitely")

    }

    @Test
    fun b() {
//        val indefinitely = hello.sayHello(HelloRequest.newBuilder().setName("name").build())
//            .onItem().transform { helloReply: HelloReply -> helloReply.message }
//            .await().indefinitely()
//
//        System.out.println(indefinitely)
    }


    @Test
    fun aTest(): Unit {
        val newSingleThreadExecutor = Executors.newSingleThreadExecutor()
        val queue = LinkedBlockingQueue<String>(10)
        val pollableSource = Uni.createFrom().item(queue::poll).runSubscriptionOn(newSingleThreadExecutor)


        val stream = pollableSource.repeat().indefinitely()


        val latch = CountDownLatch(2)

        CompletableFuture.runAsync {
            while (latch.count > 0) {
                Thread.sleep(1000)
                queue.put("hello ${latch.count}")
                latch.countDown()

            }


        }


//        val broadcast = unicastProcessor.broadcast().toAllSubscribers()
        stream.subscribe().with({
            System.out.println("Subscriber 1 $it")
        }, { er ->
            er.printStackTrace()

        })

        stream.subscribe().with({
            System.out.println("Subscriber 2 $it")
        }, { err ->
            err.printStackTrace()
        })

        latch.await()

    }


    @Test
    fun `aQueue`(): Unit {
        runBlocking {
            val queue = LinkedBlockingQueue<String>(10)
            val stopped = AtomicBoolean(false)
            val scope =  (Dispatchers.Default)
            val flow = MutableSharedFlow<String>()
            val joba = CoroutineScope(scope).launch {
                while (!stopped.get()) {
                    val iterator = queue.iterator()
                    while (iterator.hasNext()){
                        val poll = queue.poll()
                        poll?.let { flow.emit(it) }?:break
                    }
                    delay(500)
                }
            }



            val latch = CountDownLatch(2)

            CompletableFuture.runAsync {
                while (latch.count > 0) {
                    Thread.sleep(1000)
                    queue.put("hello ${latch.count}")
                    latch.countDown()

                }


            }

            val job = CoroutineScope(Dispatchers.Default).launch {
                flow.collect {
                    println("subscribe 1 $it")
                }
            }


            val job2 = CoroutineScope(Dispatchers.Default).launch {
                println("Collecting")

                flow.collect {
                    println("subscribe 2 $it")
                }
            }


            latch.await()

            CoroutineScope(Dispatchers.Default).launch {
                delay(5000)
                stopped.compareAndSet(false,true)

                joba.cancel()

            }

            joinAll(job,job2)



        }
    }

    fun CoroutineScope.counterActor() = actor<Int> {
        var counter = 0 // actor state
        for (msg in channel) { // iterate over incoming messages
            when (msg) {

            }
        }
    }
}