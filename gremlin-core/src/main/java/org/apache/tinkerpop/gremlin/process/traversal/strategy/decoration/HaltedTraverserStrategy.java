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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HaltedTraverserStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private final Class haltedTraverserFactory;
    private final boolean useReference;

    private HaltedTraverserStrategy(final Class haltedTraverserFactory) {
        if (haltedTraverserFactory.equals(DetachedFactory.class) || haltedTraverserFactory.equals(ReferenceFactory.class)) {
            this.haltedTraverserFactory = haltedTraverserFactory;
            this.useReference = ReferenceFactory.class.equals(this.haltedTraverserFactory);
        } else
            throw new IllegalArgumentException("The provided traverser detachment factory is unknown: " + haltedTraverserFactory);
    }

    public void apply(final Traversal.Admin<?, ?> traversal) {
        // do nothing as this is simply a metadata strategy
    }

    public Class getHaltedTraverserFactory() {
        return this.haltedTraverserFactory;
    }

    public <R> Traverser.Admin<R> halt(final Traverser.Admin<R> traverser) {
        if (this.useReference)
            traverser.set(ReferenceFactory.detach(traverser.get()));
        else
            traverser.set(DetachedFactory.detach(traverser.get(), true));
        return traverser;
    }

    public static final String HALTED_TRAVERSER_FACTORY = "haltedTraverserFactory";

    public static HaltedTraverserStrategy create(final Configuration configuration) {
        try {
            return new HaltedTraverserStrategy(Class.forName(configuration.getString(HALTED_TRAVERSER_FACTORY)));
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STRATEGY, HaltedTraverserStrategy.class.getCanonicalName());
        map.put(HALTED_TRAVERSER_FACTORY, this.haltedTraverserFactory.getCanonicalName());
        return new MapConfiguration(map);
    }

    ////////////

    public static HaltedTraverserStrategy detached() {
        return new HaltedTraverserStrategy(DetachedFactory.class);
    }

    public static HaltedTraverserStrategy reference() {
        return new HaltedTraverserStrategy(ReferenceFactory.class);
    }

}
