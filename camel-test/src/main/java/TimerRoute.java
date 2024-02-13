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

import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;


import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

@ApplicationScoped
public class TimerRoute extends RouteBuilder {
    @Inject
    @Named("greeting")
    String greeting;

    @Override
    public void configure() throws Exception {
        from("timer:foo?period={{timer.period}}")
                .setBody().constant("hello")
                .to("log:example");
    }


}

@ApplicationScoped
class A{

    @Inject
    CamelContext context;

    public void b(){
        Route asd = context.createFluentProducerTemplate()
                .withProcessor(context.getProcessor("asd"))
                .getCamelContext().getRoute("");
        context.stop();


    }

}