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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DummyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * Reference implementation for service calls via the {@link ServiceRegistry} and {@link Service} APIs. This step
 * can be used to start a traversal or it can be used mid-traversal with one at a time or barrier execution.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public final class CallStepPlaceholder<S, E> extends AbstractStep<S, E> implements CallStepContract<S, E>, GValueHolder<S, E> {

    private final boolean isStart;

    private final String serviceName;
    private GValue<Map<?,?>> staticParams;
    private Traversal.Admin<S,Map> mapTraversal;
    private Parameters parameters;

    public CallStepPlaceholder(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, null);
    }

    public CallStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final String service) {
        this(traversal, isStart, service, null);
    }

    public CallStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final String service, final GValue<Map<?,?>> staticParams) {
        this(traversal, isStart, service, staticParams, null);
    }

    public CallStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final String service, final GValue<Map<?,?>> staticParams,
                               final Traversal.Admin<S, Map<?,?>> mapTraversal) {
        super(traversal);

        this.isStart = isStart;
        this.serviceName = service;
        this.staticParams = staticParams == null ? GValue.of(new LinkedHashMap()) : staticParams;
        this.mapTraversal = mapTraversal == null ? null : integrateChild(mapTraversal);
        this.parameters = new Parameters();

        if (this.staticParams.isVariable()) {
            traversal.getGValueManager().register(this.staticParams);
        }
    }

    @Override
    public Service<S, E> service() {
        // throws exception for unrecognized service
        if(staticParams.isVariable()) {
            traversal.getGValueManager().pinVariable(staticParams.getName());
        }
        return getServiceRegistry().get(serviceName, isStart, staticParams.get());
    }

    @Override
    public Service<S, E> serviceGValueSafe() {
        // throws exception for unrecognized service
        return getServiceRegistry().get(serviceName, isStart, staticParams.get());
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        throw new IllegalStateException("GValuePlaceholder step is not executable");
    }

    @Override
    public Map getStaticParams() {
        if (staticParams.isVariable()) {
            traversal.getGValueManager().pinVariable(staticParams.getName());
        }
        return Collections.unmodifiableMap(this.staticParams.get());
    }

    @Override
    public GValue<Map> getStaticParamsAsGValue() {
        if (staticParams == null) {
            return GValue.ofMap(null);
        }
        return GValue.of(staticParams.getName(), Collections.unmodifiableMap(this.staticParams.get()));
    }

    @Override
    public Map getMergedParams() {
        if (mapTraversal == null && parameters.isEmpty()) {
            // static params only
            return getStaticParams();
        }

        return getMergedParams(new DummyTraverser<>(this.traversal.getTraverserGenerator()));
    }

    protected Map getMergedParams(final Traverser.Admin<S> traverser) {
        if (mapTraversal == null && parameters.isEmpty()) {
            // static params only
            return this.getStaticParams();
        }

        // merge dynamic with static params
        final Map params = new LinkedHashMap(this.getStaticParams());
        if (mapTraversal != null) params.putAll(TraversalUtil.apply(traverser, mapTraversal));
        final Object[] kvs = this.parameters.getKeyValues(traverser);
        for (int i = 0; i < kvs.length; i += 2) {
            // this will overwrite any multi-valued kvs
            params.put(kvs[i], kvs[i + 1]);
        }
        return params;
    }

    @Override
    public ServiceRegistry getServiceRegistry() {
        final Graph graph = (Graph) this.traversal.getGraph().get();
        return graph.getServiceRegistry();
    }

    @Override
    public void reset() {
        super.reset();
        if (mapTraversal != null)
            mapTraversal.reset();
        parameters.getTraversals().forEach(Traversal.Admin::reset);
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CallStepPlaceholder<?, ?> that = (CallStepPlaceholder<?, ?>) o;
        return isStart == that.isStart &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(staticParams, that.staticParams) &&
                Objects.equals(mapTraversal, that.mapTraversal) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isStart, serviceName, staticParams, mapTraversal, parameters);
    }

    @Override
    public CallStepPlaceholder<S, E> clone() {
        final CallStepPlaceholder<S, E> clone = (CallStepPlaceholder<S, E>) super.clone();
        clone.staticParams = staticParams != null ? staticParams.clone() : null;
        clone.mapTraversal = mapTraversal != null ? mapTraversal.clone() : null;
        clone.parameters = parameters.clone();
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

    @Override
    public CallStep<S, E> asConcreteStep() {
        CallStep<S, E> step = new CallStep<>(traversal, isStart, serviceName, staticParams.get(), mapTraversal);
        for (Map.Entry<Object, List<Object>> entry : parameters.getRaw().entrySet()) {
            for (Object value : entry.getValue()) {
                step.configure(entry.getKey(), value);
            }
        }
        return step;
    }

    @Override
    public boolean isParameterized() {
        return staticParams.isVariable();
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (name.equals(staticParams.getName())) {
            staticParams = GValue.of(name, (Map<?,?>) value);
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        return staticParams.isVariable() ? List.of(staticParams) : Collections.emptyList();
    }
}
