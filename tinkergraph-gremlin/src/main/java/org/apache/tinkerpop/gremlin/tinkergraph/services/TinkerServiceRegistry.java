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
package org.apache.tinkerpop.gremlin.tinkergraph.services;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * TinkerGraph services are currently just "toy" services, used to demonstrate and to test.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public class TinkerServiceRegistry extends ServiceRegistry {

    private final TinkerGraph graph;

    public TinkerServiceRegistry(final TinkerGraph graph) {
        this.graph = graph;
    }

    public TinkerServiceFactory registerService(final TinkerServiceFactory service) {
        super.registerService(service);
        return service;
    }

    public <I, R> LambdaServiceFactory<I, R> registerLambdaService(final String name) {
        return (LambdaServiceFactory) registerService(new LambdaServiceFactory(graph, name));
    }

    public abstract static class TinkerServiceFactory<I, R> implements ServiceFactory<I, R> {
        protected final TinkerGraph graph;
        protected final String name;
        protected final Map describeParams = new LinkedHashMap();
        protected final Map<Type,Set<TraverserRequirement>> requirements = new LinkedHashMap<>();

        protected TinkerServiceFactory(final TinkerGraph graph, final String name) {
            this.graph = graph;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        public TinkerServiceFactory addDescribeParams(final Map describeParams) {
            this.describeParams.putAll(describeParams);
            return this;
        }

        public TinkerServiceFactory addRequirements(final Type type, final TraverserRequirement... requirements) {
            final Set<TraverserRequirement> typeRequirements = this.requirements.computeIfAbsent(type, x -> new LinkedHashSet<>());
            for (TraverserRequirement requirement : requirements) typeRequirements.add(requirement);
            return this;
        }

        @Override
        public Set<TraverserRequirement> getRequirements(final Type type) {
            return requirements.getOrDefault(type, Collections.emptySet());
        }

        @Override
        public Map describeParams() {
            return describeParams;
        }
    }

    public abstract static class TinkerService<I, R> implements Service<I, R> {
        protected final TinkerServiceFactory<I, R> serviceFactory;

        protected TinkerService(final TinkerServiceFactory<I, R> serviceFactory) {
            this.serviceFactory = serviceFactory;
        }

        public TinkerService addRequirements(final TraverserRequirement... requirements) {
            serviceFactory.addRequirements(getType(), requirements);
            return this;
        }

        @Override
        public Set<TraverserRequirement> getRequirements() {
            return serviceFactory.getRequirements(getType());
        }
    }

    public static class LambdaServiceFactory<I, R> extends TinkerServiceFactory<I, R> {

        public interface Options {
            /**
             * Dynamic configuration of execution type.
             */
            String TYPE = "Type";
            Type DEFAULT_TYPE = Type.Streaming;

            /**
             * Dynamic configuration of chunk size for barriers.
             */
            String CHUNK_SIZE = "ChunkSize";
            Integer DEFAULT_CHUNK_SIZE = Integer.MAX_VALUE;
        }

        private Map<Type, Service<I, R>> lambdas = new LinkedHashMap<>();

        public LambdaServiceFactory(final TinkerGraph graph, final String name) {
            super(graph, name);
        }

        @Override
        public Set<Type> getSupportedTypes() {
            return lambdas.keySet();
        }

        @Override
        public LambdaServiceFactory addDescribeParams(final Map describeParams) {
            return (LambdaServiceFactory) super.addDescribeParams(describeParams);
        }

        public LambdaStartService addStartLambda(final BiFunction<ServiceCallContext, Map, Iterator<R>> lambda) {
            final LambdaStartService<I, R> service = new LambdaStartService<>(this, lambda);
            lambdas.put(Type.Start, service);
            return service;
        }

        public LambdaStreamingService addStreamingLambda(final TriFunction<ServiceCallContext, Traverser.Admin<I>, Map, Iterator<R>> lambda) {
            final LambdaStreamingService<I, R> service = new LambdaStreamingService<>(this, lambda);
            lambdas.put(Type.Streaming, service);
            return service;
        }

        public LambdaBarrierService addBarrierLambda(final TriFunction<ServiceCallContext, TraverserSet<I>, Map, Iterator<R>> lambda) {
            final LambdaBarrierService<I, R> service = new LambdaBarrierService<>(this, lambda);
            lambdas.put(Type.Barrier, service);
            return service;
        }

        public LambdaBarrierService addBarrierLambda(final TriFunction<ServiceCallContext, TraverserSet<I>, Map, Iterator<R>> lambda, final int maxChunkSize) {
            final LambdaBarrierService<I, R> service = new LambdaBarrierService<>(this, lambda, maxChunkSize);
            lambdas.put(Type.Barrier, service);
            return service;
        }


        @Override
        public Service<I, R> createService(final boolean isStart, final Map params) {
            if (isStart) {
                if (supports(Type.Start)) {
                    return getService(Type.Start, params);
                } else {
                    throw new UnsupportedOperationException(Service.Exceptions.cannotStartTraversal);
                }
            } else {
                if (supports(Type.Streaming, Type.Barrier)) {
                    /*
                     * This service supports both barrier and streaming execution. Look to the parameters for guidance.
                     */
                    final Type type = (Type) params.getOrDefault(Options.TYPE, Options.DEFAULT_TYPE);
                    return getService(type, params);
                } else if (supports(Type.Streaming)) {
                    return getService(Type.Streaming, params);
                } else if (supports(Type.Barrier)) {
                    return getService(Type.Barrier, params);
                } else {
                    throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
                }
            }
        }

        private Service<I, R> getService(final Type type, final Map params) {
            if (type == Type.Streaming || type == Type.Start) {
                return lambdas.get(type);
            } else {
                final LambdaBarrierService service = (LambdaBarrierService) lambdas.get(Type.Barrier);
                // check for dynamic chunk size
                if (params.containsKey(Options.CHUNK_SIZE))
                    return service.clone((int) params.get(Options.CHUNK_SIZE));
                else
                    return service;
            }
        }

        /**
         * Does this service support all of the specified types.
         */
        private boolean supports(final Type... types) {
            for (Type type : types) {
                if (!lambdas.containsKey(type)) {
                    return false;
                }
            }
            return true;
        }

    }

    public static class LambdaStartService<I, R> extends TinkerService<I, R> {
        private final BiFunction<ServiceCallContext, Map, Iterator<R>> lambda;

        public LambdaStartService(final LambdaServiceFactory<I, R> factory,
                                  final BiFunction<ServiceCallContext, Map, Iterator<R>> lambda) {
            super(factory);
            this.lambda = lambda;
        }

        @Override
        public Type getType() {
            return Type.Start;
        }

        @Override
        public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
            return CloseableIterator.of(lambda.apply(ctx, params));
        }
    }

    public static class LambdaStreamingService<I, R> extends TinkerService<I, R> {
        private final TriFunction<ServiceCallContext, Traverser.Admin<I>, Map, Iterator<R>> lambda;

        public LambdaStreamingService(final LambdaServiceFactory<I, R> factory,
                                      final TriFunction<ServiceCallContext, Traverser.Admin<I>, Map, Iterator<R>> lambda) {
            super(factory);
            this.lambda = lambda;
        }

        @Override
        public Type getType() {
            return Type.Streaming;
        }

        @Override
        public CloseableIterator<R> execute(final ServiceCallContext ctx, final Traverser.Admin<I> in, final Map params) {
            final Object result = lambda.apply(ctx, in, params);
            return CloseableIterator.of(result instanceof Iterator ? (Iterator<R>) result : IteratorUtils.of((R) result));
        }
    }

    public static class LambdaBarrierService<I, R> extends TinkerService<I, R> {
        private final TriFunction<ServiceCallContext, TraverserSet<I>, Map, Iterator<R>> lambda;
        private final int maxChunkSize;

        public LambdaBarrierService(final LambdaServiceFactory<I, R> factory,
                                    final TriFunction<ServiceCallContext, TraverserSet<I>, Map, Iterator<R>> lambda) {
            this(factory, lambda, Integer.MAX_VALUE);
        }

        public LambdaBarrierService(final LambdaServiceFactory<I, R> factory,
                                    final TriFunction<ServiceCallContext, TraverserSet<I>, Map, Iterator<R>> lambda,
                                    final int maxChunkSize) {
            super(factory);
            this.lambda = lambda;
            this.maxChunkSize = maxChunkSize;
        }

        public LambdaBarrierService clone(final int maxChunkSize) {
            return new LambdaBarrierService((LambdaServiceFactory) serviceFactory, lambda, maxChunkSize);
        }

        @Override
        public Type getType() {
            return Type.Barrier;
        }

        @Override
        public int getMaxBarrierSize() {
            return maxChunkSize;
        }

        @Override
        public CloseableIterator<R> execute(final ServiceCallContext ctx, final TraverserSet<I> in, final Map params) {
            final Object result = lambda.apply(ctx, in, params);
            return CloseableIterator.of(result instanceof Iterator ? (Iterator<R>) result : IteratorUtils.of((R) result));
        }
    }

}
