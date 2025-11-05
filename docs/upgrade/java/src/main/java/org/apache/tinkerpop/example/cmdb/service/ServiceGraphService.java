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
package org.apache.tinkerpop.example.cmdb.service;

import jakarta.inject.Singleton;
import org.apache.tinkerpop.example.cmdb.graph.GremlinGateway;
import org.apache.tinkerpop.example.cmdb.graph.TraversalBuilders;

import java.util.List;
import java.util.Map;

@Singleton
public class ServiceGraphService {

    private final GremlinGateway gateway;

    public ServiceGraphService(GremlinGateway gateway) {
        this.gateway = gateway;
    }

    public List<Map<Object, Object>> dependencies(String serviceId, int depth) {
        return gateway.fetch(TraversalBuilders.serviceDependencies(serviceId, depth));
    }

    public List<Map<Object, Object>> owners(String serviceId) {
        return gateway.fetch(TraversalBuilders.serviceOwners(serviceId));
    }
}
