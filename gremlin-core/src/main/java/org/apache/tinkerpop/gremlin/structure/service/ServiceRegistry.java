/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.service;

import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.DirectoryService;

/**
 * A basic service registry implementation used by {@link CallStep}. This service registry contains one meta-service -
 * the {@link DirectoryService}, which is used to list and describe registered callable services.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public class ServiceRegistry implements DirectoryService, AutoCloseable {

    /**
     * Empty instance, for the {@link Graph} interface.
     */
    public static final ServiceRegistry EMPTY = new ServiceRegistry() {
        @Override
        public ServiceFactory registerService(final ServiceFactory service) {
            throw new UnsupportedOperationException();
        }
    };

    private final LinkedHashMap<String, ServiceFactory> services = new LinkedHashMap<>();

    public ServiceRegistry() {}

    /**
     * Register a callable service.
     */
    public ServiceFactory registerService(final ServiceFactory serviceFactory) {
        Objects.requireNonNull(serviceFactory);
        services.put(serviceFactory.getName(), serviceFactory);
        return serviceFactory;
    }

    /**
     * Check for non-null and registered.
     */
    public void checkRegisteredService(final String service) {
        if (service != null && !services.containsKey(service))
            throw new IllegalArgumentException("Unrecognized service: " + service);
    }

    /**
     * Lookup a service by name.
     */
    public Service get(final String service, final boolean isStart, final Map params) {
        if (service == null || service.equals(DirectoryService.NAME))
            return this;

        checkRegisteredService(service);
        return services.get(service).createService(isStart, params);
    }

    /**
     * {@link DirectoryService} execution. List or describe the registered callable services.
     */
    @Override
    public CloseableIterator execute(ServiceCallContext ctx, final Map params) {
        final boolean verbose = (boolean) params.getOrDefault(Params.VERBOSE, false);
        final String serviceName = (String) params.get(Params.SERVICE);

        return CloseableIterator.of(
                services.values().stream()
                        .filter(s -> serviceName == null || s.getName().equals(serviceName))
                        .map(s -> verbose ? describe(s) : s.getName()).iterator());
    }

    /**
     * Provide a service description for the supplied service.
     */
    protected String describe(final ServiceFactory service) {
        final Map description = new LinkedHashMap();
        description.put("name", service.getName());
        final Map<Type,Set<TraverserRequirement>> types = new LinkedHashMap<>();
        for (Type type : (Set<Type>) service.getSupportedTypes()) {
            types.put(type, service.getRequirements(type));
        }
        description.put("type:[requirements]:", types);
        description.put("params", service.describeParams());

        final ObjectMapper om = new ObjectMapper();
        try {
            return om.writeValueAsString(description);
        } catch (JsonProcessingException ex) {
            return description.toString();
        }
    }

    @Override
    public void close() {
        services.values().forEach(ServiceFactory::close);
    }
}
