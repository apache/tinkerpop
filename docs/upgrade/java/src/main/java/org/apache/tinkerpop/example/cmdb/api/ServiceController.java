/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tinkerpop.example.cmdb.api;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.QueryValue;
import org.apache.tinkerpop.example.cmdb.service.ServiceGraphService;

import java.util.List;
import java.util.Map;

@Controller("/services")
public class ServiceController {

    private final ServiceGraphService service;

    public ServiceController(ServiceGraphService service) {
        this.service = service;
    }

    @Get("/{id}/dependencies{?depth}")
    public HttpResponse<List<Map<Object, Object>>> dependencies(@PathVariable String id,
                                                                @QueryValue(defaultValue = "2") int depth) {
        return HttpResponse.ok(service.dependencies(id, depth));
    }

    @Get("/{id}/owners")
    public HttpResponse<List<Map<Object, Object>>> owners(@PathVariable String id) {
        return HttpResponse.ok(service.owners(id));
    }
}
