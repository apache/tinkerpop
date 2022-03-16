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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;

/**
 * Service call with I input type and R return type. Services can return {@link Traverser}s or raw values (which will be
 * converted into Traversers by {@link CallStep}.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public interface Service<I, R> extends AutoCloseable {

    /**
     * The service factory creates instances of callable services based on the desired execution type. Some services
     * (e.g. full text search) might be run at the start of a traversal or mid-traversal. The service factory
     * will create an instance based on how the service will be executed. This leaves open the possibility for
     * a single named service to support multiple execution modes (streaming vs. chunked vs. all-at-once) and
     * dynamically choose one based on the location in the traversal of the service call and the static parameters
     * supplied (this allow for dynamic chunk size).
     */
    interface ServiceFactory<I, R> extends AutoCloseable {

        /**
         * Get the name of this service.
         */
        String getName();

        /**
         * Get the execution modes that it supports.
         */
        Set<Type> getSupportedTypes();

        /**
         * Return a description of any service call parameters.
         */
        default Map describeParams() {
            return Collections.emptyMap();
        }

        /**
         * Return any {@link TraverserRequirement}s necessary for this service call for each execution {@link Type}
         * it supports.
         */
        default Map<Type,Set<TraverserRequirement>> getRequirementsByType() {
            return Collections.emptyMap();
        }

        /**
         * Return any {@link TraverserRequirement}s necessary for this service call for the supplied execution
         * {@link Type}.
         */
        default Set<TraverserRequirement> getRequirements(final Type type) {
            return getRequirementsByType().getOrDefault(type, Collections.emptySet());
        }

        /**
         * Create a Service call instance.
         *
         * @param isStart true if the call is being used to start a traversal
         * @param params the static params provided to service call (if any)
         * @return the service call instance
         */
        Service<I, R> createService(final boolean isStart, final Map params);

        /**
         * Service factories can implement cleanup/shutdown procedures here.
         */
        @Override
        default void close() {}

    }

    /**
     * Service calls can appear at the start of a traversal or mid-traversal. If mid-traversal, they can operate in a
     * streaming fashion or as a barrier. A Tinkerpop service instance can only operate in one of those three modes,
     * however if the underlying service can do multiple modes, it can be registered using a {@link ServiceFactory}
     * capable of providing service instances for the supported types.
     */
    enum Type {
        /**
         * Start the traversal with no upstream input.
         */
        Start,

        /**
         * Mid-traversal with streaming input.
         */
        Streaming,

        /**
         * Mid-traversal with all-at-once input.
         */
        Barrier;
    }

    /**
     * Return the {@link Type} of service call.
     */
    Type getType();

    /**
     * Return any {@link TraverserRequirement}s necessary for this service call.
     */
    default Set<TraverserRequirement> getRequirements() {
        return Collections.emptySet();
    }

    /**
     * True if Start type.
     */
    default boolean isStart() {
        return getType() == Type.Start;
    }

    /**
     * True if Streaming type.
     */
    default boolean isStreaming() {
        return getType() == Type.Streaming;
    }

    /**
     * True if Barrier type.
     */
    default boolean isBarrier() {
        return getType() == Type.Barrier;
    }

    /**
     * Return the max barrier size. Default is all upstream solutions.
     */
    default int getMaxBarrierSize() {
        return Integer.MAX_VALUE;
    }

    /**
     * Execute a Start service call.
     */
    default CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        throw new UnsupportedOperationException(Exceptions.cannotStartTraversal);
    }

    /**
     * Execute a Streaming service call with one upstream input.
     */
    default CloseableIterator<R> execute(final ServiceCallContext ctx, final Traverser.Admin<I> in, final Map params)  {
        throw new UnsupportedOperationException(Exceptions.doesNotSupportStreaming);
    }

    /**
     * Execute a Barrier service call with all upstream input.
     */
    default CloseableIterator<R> execute(final ServiceCallContext ctx, final TraverserSet<I> in, final Map params)  {
        throw new UnsupportedOperationException(Exceptions.doesNotSupportBarrier);
    }

    /**
     * Services can implement cleanup/shutdown procedures here.
     */
    @Override
    default void close() {}

    /**
     * Meta-service to list and describe registered callable services.
     */
    interface DirectoryService<I> extends Service<I, String>, ServiceFactory {

        String NAME = "--list";

        interface Params {

            String SERVICE = "service";

            String VERBOSE = "verbose";

            Map describeParams = asMap(
                    SERVICE, "The name of the service to describe",
                    VERBOSE, "Flag to provide a detailed service description"
            );

        }

        @Override
        default String getName() {
            return NAME;
        }

        @Override
        default Type getType() {
            return Type.Start;
        }

        @Override
        default Set<Type> getSupportedTypes() {
            return Collections.singleton(Type.Start);
        }

        @Override
        default Map describeParams() {
            return Params.describeParams;
        }

        @Override
        default Service createService(final boolean isStart, final Map params) {
            if (!isStart) {
                throw new UnsupportedOperationException(Exceptions.directoryStartOnly);
            }
            return this;
        }

        /**
         * List or describe any registered callable services.
         */
        CloseableIterator<String> execute(ServiceCallContext ctx, Map params);

        @Override
        default void close() {}

    }

    /**
     * Context information for service call invocation. Useful for Barrier services that want to produce their own
     * Traversers that maintain path information.
     */
    class ServiceCallContext implements Cloneable {

        private final Traversal.Admin traversal;
        private final Step step;

        public ServiceCallContext(final Traversal.Admin traversal, final Step step) {
            this.traversal = traversal;
            this.step = step;
        }

        public Traversal.Admin getTraversal() {
            return traversal;
        }

        public Step getStep() {
            return step;
        }

        public <T> Traverser<T> generateTraverser(final T value) {
            return traversal.getTraverserGenerator().generate(value, step, 1l);
        }

        public <T> Traverser.Admin<T> split(final Traverser.Admin<T> t, final T value) {
            return t.split(value, step);
        }

        @Override
        public ServiceCallContext clone() {
            return new ServiceCallContext(traversal, step);
        }
    }

    interface Exceptions {

        String cannotStartTraversal = "This service cannot be used to start a traversal.";

        String cannotUseMidTraversal = "This service cannot be used mid-traversal.";

        String doesNotSupportStreaming = "This service does not support streaming execution.";

        String doesNotSupportBarrier = "This service does not support barrier execution.";

        String directoryStartOnly = "Directory service can only be used to start a traversal.";

    }

}
