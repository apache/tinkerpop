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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DummyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.util.CloseableIterator.EmptyCloseableIterator;
import static org.apache.tinkerpop.gremlin.structure.service.Service.ServiceCallContext;

/**
 * Reference implementation for service calls via the {@link ServiceRegistry} and {@link Service} APIs. This step
 * can be used to start a traversal or it can be used mid-traversal with one at a time or barrier execution.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public final class CallStep<S, E> extends AbstractStep<S, E> implements TraversalParent, AutoCloseable, Configuring {

    private final boolean isStart;
    private boolean first = true;

    private ServiceCallContext ctx;
    private String serviceName;
    private Service<S, E> service;
    private GValue<Map> staticParams;
    private Traversal.Admin<S,Map> mapTraversal;
    private Parameters parameters;

    private transient Traverser.Admin<S> head = null;
    private transient CloseableIterator iterator = EmptyCloseableIterator.instance();

    public CallStep(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, null);
    }

    public CallStep(final Traversal.Admin traversal, final boolean isStart, final String service) {
        this(traversal, isStart, service, (Map) null);
    }

    public CallStep(final Traversal.Admin traversal, final boolean isStart, final String service, final Map staticParams) {
        this(traversal, isStart, service, GValue.ofMap(null, staticParams));
    }

    public CallStep(final Traversal.Admin traversal, final boolean isStart, final String service, final GValue<Map> staticParams) {
        this(traversal, isStart, service, staticParams, null);
    }

    public CallStep(final Traversal.Admin traversal, final boolean isStart, final String service, final Map staticParams,
                    final Traversal.Admin<S, Map> mapTraversal) {
        this(traversal, isStart, service, GValue.ofMap(null, staticParams), mapTraversal);
    }

    public CallStep(final Traversal.Admin traversal, final boolean isStart, final String service, final GValue<Map> staticParams,
                    final Traversal.Admin<S, Map> mapTraversal) {
        super(traversal);

        this.isStart = isStart;
        this.serviceName = service;
        this.staticParams = staticParams == null || staticParams.isNull() ? GValue.ofMap(staticParams.getName(), new LinkedHashMap()) : staticParams;
        this.mapTraversal = mapTraversal == null ? null : integrateChild(mapTraversal);
        this.parameters = new Parameters();
        this.ctx = new ServiceCallContext(traversal, this);
    }

    protected Service<S, E> service() {
        // throws exception for unrecognized service
        return service != null ? service : (service = getServiceRegistry().get(serviceName, isStart, staticParams.get()));
    }

    public String getServiceName() {
        return serviceName;
    }

    public GValue<Map> getStaticParams() {
        return staticParams;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (isStart && first) {
            first = false;

            /*
             * Start of a traversal (no input). Obtain the one-time service iterator.
             */

            if (this.starts.hasNext()) {
                throw new IllegalStateException("This service must be called without input: " + serviceName);
            }

            this.iterator = start();
        }

        while (true) {
            if (this.iterator.hasNext()) {

                /*
                 * Still draining the current iterator.
                 */

                final Object next = this.iterator.next();
                if (next instanceof Traverser.Admin) {
                    /*
                     * Service is producing its own traversers (or is a pass-through on the input). Possible for
                     * streaming or barrier.
                     */
                    return (Traverser.Admin<E>) next;
                } else if (this.head != null) {
                    /*
                     * Streaming service mapping input (head) to non-traverser output. This applies to streaming
                     * service execution only.
                     */
                    return this.head.split((E) next, this);
                } else {
                    /*
                     * Barrier service producing non-traverser output, we have no head to split against. This loses
                     * all path information.
                     */
                    return this.traversal.getTraverserGenerator().generate(next, (Step) this, 1l);
                }
            } else {

                /*
                 * Time to obtain another iterator from upstream input.
                 */

                closeIterator();
                if (!this.starts.hasNext()) {
                    // no more input
                    throw FastNoSuchElementException.instance();
                }
                if (service().isBarrier()) {
                    /*
                     * Barrier service - gather upstream input and call.
                     */
                    final TraverserSet<S> traverserSet = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
                    final int maxBarrierSize = service().getMaxBarrierSize();
                    if (maxBarrierSize == Integer.MAX_VALUE) {
                        // all-at-once
                        this.starts.forEachRemaining(traverserSet::add);
                    } else {
                        // chunked
                        while (this.starts.hasNext() && traverserSet.size() < maxBarrierSize) {
                            traverserSet.add(this.starts.next());
                        }
                    }
                    this.iterator = this.flatMap(traverserSet);
                } else {
                    /*
                     * Streaming service, mark the next start and call.
                     */
                    this.head = this.starts.next();
                    this.iterator = this.flatMap(this.head);
                }
            }
        }
    }

    @Override
    public void close() {
        closeIterator();
        if (service != null)
            service.close();
        service = null;
    }

    protected void closeIterator() {
        CloseableIterator.closeIterator(iterator);
        this.iterator = EmptyCloseableIterator.instance();
    }

    public Map getMergedParams() {
        if (mapTraversal == null && parameters.isEmpty()) {
            // static params only
            return Collections.unmodifiableMap(this.staticParams.get());
        }

        return getMergedParams(new DummyTraverser(this.traversal.getTraverserGenerator()));
    }

    protected Map getMergedParams(final Traverser.Admin<S> traverser) {
        if (mapTraversal == null && parameters.isEmpty()) {
            // static params only
            return Collections.unmodifiableMap(this.staticParams.get());
        }

        // merge dynamic with static params
        final Map params = new LinkedHashMap(this.staticParams.get());
        if (mapTraversal != null) params.putAll(TraversalUtil.apply(traverser, mapTraversal));
        final Object[] kvs = this.parameters.getKeyValues(traverser);
        for (int i = 0; i < kvs.length; i += 2) {
            // this will overwrite any multi-valued kvs
            params.put(kvs[i], kvs[i + 1]);
        }
        return params;
    }

    protected Map getMergedParams(final TraverserSet<S> traverserSet) {
        if (mapTraversal == null && parameters.isEmpty()) {
            // static params only
            return Collections.unmodifiableMap(this.staticParams.get());
        }

        /*
         * Dynamic params with a barrier service. We need to reduce to one set of params. For now just disallow
         * multiple property sets. Also could be sensible to group traversers by parameter set.
         */
        final Set<Map> paramsSet = new HashSet<>();
        for (final Traverser.Admin<S> traverser : traverserSet) {
            paramsSet.add(getMergedParams(traverser));
        }
        if (paramsSet.size() > 1) {
            throw new UnsupportedOperationException("Cannot use multiple dynamic parameter sets with a barrier service call.");
        }
        return paramsSet.iterator().next();
    }

    protected CloseableIterator start() {
        final Map params = getMergedParams();
        return service().execute(this.ctx, params);
    }

    protected CloseableIterator flatMap(final Traverser.Admin<S> traverser) {
        final Map params = getMergedParams(traverser);
        return service().execute(this.ctx, traverser, params);
    }

    protected CloseableIterator flatMap(final TraverserSet<S> traverserSet) {
        final Map params = getMergedParams(traverserSet);
        return service().execute(this.ctx, traverserSet, params);
    }

    protected ServiceRegistry getServiceRegistry() {
        final Graph graph = (Graph) this.traversal.getGraph().get();
        return graph.getServiceRegistry();
    }

    @Override
    public void reset() {
        super.reset();
        first = true;
        if (mapTraversal != null)
            mapTraversal.reset();
        parameters.getTraversals().forEach(Traversal.Admin::reset);

        closeIterator();
        head = null;
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        final List<Traversal.Admin<S, E>> children = new ArrayList<>();
        if (mapTraversal != null) children.add((Traversal.Admin<S, E>) mapTraversal);
        children.addAll(parameters.getTraversals());
        return children;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (mapTraversal != null)
            this.integrateChild(mapTraversal);
        parameters.getTraversals().forEach(this::integrateChild);
        ctx = new ServiceCallContext(parentTraversal, this);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements();
        requirements.addAll(service().getRequirements());
        return requirements;
    }

    @Override
    public String toString() {
        final ArrayList args = new ArrayList();
        args.add(serviceName);
        if (!staticParams.get().isEmpty())
            args.add(staticParams);
        if (mapTraversal != null)
            args.add(mapTraversal);
        if (!parameters.isEmpty())
            args.add(parameters);
        return StringFactory.stepString(this, args.toArray());
    }

    @Override
    public int hashCode() {
        int hashCode = super.hashCode() ^ Objects.hashCode(this.serviceName);
        if (!staticParams.get().isEmpty())
            hashCode ^= staticParams.get().hashCode();
        if (mapTraversal != null)
            hashCode ^= mapTraversal.hashCode();
        if (!parameters.isEmpty())
            hashCode ^= parameters.hashCode();
        return hashCode;
    }

    @Override
    public CallStep<S, E> clone() {
        final CallStep<S, E> clone = (CallStep<S, E>) super.clone();
        clone.mapTraversal = mapTraversal != null ? mapTraversal.clone() : null;
        clone.parameters = parameters.clone();
        clone.ctx = new ServiceCallContext(traversal, clone);
        clone.iterator = EmptyCloseableIterator.instance();
        clone.head = null;
        return clone;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }
}
