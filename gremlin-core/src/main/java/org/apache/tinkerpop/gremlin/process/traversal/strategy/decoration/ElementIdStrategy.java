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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Provides a degree of control over element identifier assignment as some graphs don't provide that feature. This
 * strategy provides for identifier assignment by enabling users to utilize vertex and edge indices under the hood,
 * thus simulating that capability.
 * <p/>
 * By default, when an identifier is not supplied by the user, newly generated identifiers are {@link UUID} objects.
 * This behavior can be overridden by setting the {@link Builder#idMaker(Supplier)}.
 * <p/>
 * Unless otherwise specified the identifier is stored in the {@code __id} property.  This can be changed by setting
 * the {@link Builder#idPropertyKey(String)}
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ElementIdStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    public static final ElementIdStrategy INSTANCE = ElementIdStrategy.build().create();

    private final String idPropertyKey;

    private final Supplier<Object> idMaker;

    private ElementIdStrategy(final String idPropertyKey, final Supplier<Object> idMaker) {
        this.idPropertyKey = idPropertyKey;
        this.idMaker = idMaker;
    }

    public String getIdPropertyKey() {
        return idPropertyKey;
    }

    public Supplier<Object> getIdMaker() {
        return idMaker;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClass(HasStep.class, traversal).stream().
                forEach(hasStep -> ((HasStep<?>) hasStep).getHasContainers().stream().
                            filter(container -> container.getKey().equals(T.id.getAccessor())).
                                     forEach(container -> container.setKey(this.idPropertyKey)));

        if (traversal.getStartStep() instanceof GraphStep) {
            final GraphStep graphStep = (GraphStep) traversal.getStartStep();
            // only need to apply the custom id if ids were assigned - otherwise we want the full iterator.
            // note that it is then only necessary to replace the step if the id is a non-element.  other tests
            // in the suite validate that items in getIds() is uniform so it is ok to just test the first item
            // in the list.
            final Object[] ids = graphStep.getIdsAsValues();
            if (ids.length > 0 && !(ids[0] instanceof Element)) {
                if (graphStep instanceof HasContainerHolder)
                    ((HasContainerHolder) graphStep).addHasContainer(new HasContainer(this.idPropertyKey, P.within(Arrays.asList(ids))));
                else
                    TraversalHelper.insertAfterStep(new HasStep(traversal, new HasContainer(this.idPropertyKey, P.within(Arrays.asList(ids)))), graphStep, traversal);
                graphStep.clearIds();
            }
        }
        TraversalHelper.getStepsOfAssignableClass(IdStep.class, traversal).stream().forEach(step -> {
            TraversalHelper.replaceStep(step, new PropertiesStep(traversal, PropertyType.VALUE, idPropertyKey), traversal);
        });

        // in each case below, determine if the T.id is present and if so, replace T.id with the idPropertyKey or if
        // it is not present then shove it in there and generate an id
        traversal.getSteps().forEach(step -> {
            if (step instanceof AddVertexStep || step instanceof AddVertexStartStep || step instanceof AddEdgeStep) {
                final Parameterizing parameterizing = (Parameterizing) step;
                if (parameterizing.getParameters().contains(T.id))
                    parameterizing.getParameters().rename(T.id, this.idPropertyKey);
                else if (!parameterizing.getParameters().contains(this.idPropertyKey))
                    parameterizing.getParameters().set(null, this.idPropertyKey, idMaker.get());
            }
        });
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategyString(this);
    }

    public final static class Builder {
        private static final Supplier<Object> DEFAULT_ID_MAKER = () -> UUID.randomUUID().toString();

        private String idPropertyKey = "__id";

        private Supplier<Object> idMaker = DEFAULT_ID_MAKER;

        private Builder() {
        }

        /**
         * Creates a new unique identifier for the next created {@link Element}.
         */
        public Builder idMaker(final Supplier<Object> idMaker) {
            this.idMaker = idMaker;
            return this;
        }

        /**
         * This key within which to hold the user-specified identifier.  This field should be indexed by the
         * underlying graph.
         */
        public Builder idPropertyKey(final String idPropertyKey) {
            this.idPropertyKey = idPropertyKey;
            return this;
        }

        public ElementIdStrategy create() {
            return new ElementIdStrategy(idPropertyKey, idMaker);
        }
    }

    public static final String ID_PROPERTY_KEY = "idPropertyKey";
    public static final String ID_MAKER = "idMaker";

    public static ElementIdStrategy create(final Configuration configuration) {
        final ElementIdStrategy.Builder builder = ElementIdStrategy.build();
        if (configuration.containsKey(ID_MAKER))
            builder.idMaker((Supplier) configuration.getProperty(ID_MAKER));
        if (configuration.containsKey(ID_PROPERTY_KEY))
            builder.idPropertyKey(configuration.getString(ID_PROPERTY_KEY));
        return builder.create();
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STRATEGY, ElementIdStrategy.class.getCanonicalName());
        map.put(ID_PROPERTY_KEY, this.idPropertyKey);
        // The idMaker is not serializable. The default idMaker is excluded from the configuration to allow for remote
        // serialization. Custom idMakers are only supported in the config for embedded usages.
        if (!idMaker.equals(Builder.DEFAULT_ID_MAKER)) {
            map.put(ID_MAKER, this.idMaker);
        }
        return new MapConfiguration(map);
    }

    public static ElementIdStrategy instance() {
        return INSTANCE;
    }
}
