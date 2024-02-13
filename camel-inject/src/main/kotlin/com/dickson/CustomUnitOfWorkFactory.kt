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

import org.apache.camel.Exchange
import org.apache.camel.impl.engine.MDCUnitOfWork
import org.apache.camel.spi.UnitOfWork
import org.apache.camel.spi.UnitOfWorkFactory
import org.apache.camel.AsyncCallback
import org.apache.camel.Processor
import org.slf4j.MDC


class CustomUnitOfWorkFactory() : UnitOfWorkFactory{
    override fun createUnitOfWork(exchange: Exchange): UnitOfWork {
        System.out.println("Unit work factory")
       return MyUnitOfWork(exchange)
    }
}

class MyUnitOfWork(val exchange:Exchange) : MDCUnitOfWork(exchange, exchange.getContext().getInflightRepository(), "", false, false){
    override fun beforeProcess(processor: Processor, exchange: Exchange, callback: AsyncCallback?): AsyncCallback? {
        val value = exchange.getIn().getHeader(
            Exchange.BREADCRUMB_ID,
            String::class.java
        )
        if (value != null) {
            MDC.put("camel." + Exchange.BREADCRUMB_ID, value)
        }
        return super.beforeProcess(processor, exchange, callback)
    }


}

