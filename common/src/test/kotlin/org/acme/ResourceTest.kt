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

import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.Uni
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpMethod
import io.vertx.mutiny.core.Vertx
import org.junit.jupiter.api.Test
import javax.inject.Inject
import io.vertx.ext.web.client.WebClient




@QuarkusTest
class ResourceTest {

    @Inject
    lateinit var vertx: Vertx

    @Test
    fun aTest(){
        val client = vertx.createHttpClient()
        val request = client.request(HttpMethod.GET, "http://localhost:8080/hello/why").await().indefinitely()
        val response = request.send().await().indefinitely()
        response.handler {
            println(it)

        }



    }
}