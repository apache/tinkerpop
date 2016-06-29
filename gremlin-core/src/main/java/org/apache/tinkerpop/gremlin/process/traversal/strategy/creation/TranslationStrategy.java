/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.creation;

import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.StepTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TranslationStrategy extends AbstractTraversalStrategy<TraversalStrategy.CreationStrategy> implements TraversalStrategy.CreationStrategy {

    private final TraversalSource traversalSource;
    private final Class anonymousTraversalClass;


    public TranslationStrategy(final TraversalSource traversalSource, final Class anonymousTraversalClass) {
        this.traversalSource = traversalSource;
        this.anonymousTraversalClass = anonymousTraversalClass;
        this.createAnonymousTraversalFunction();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))
            return;
        try {
            // reset __ back to default
            this.destroyAnonymousTraversalFunction();

            // if the graph is RemoteGraph, RemoteStrategy will send the traversal
            if (traversal.getGraph().isPresent() && traversal.getGraph().get() instanceof RemoteGraph)
                return;

            // translate the traversal and add its steps to this traversal
            final ScriptEngine scriptEngine = ScriptEngineCache.get(traversal.getStrategies().getTranslator().getTargetLanguage());
            final Bindings bindings = scriptEngine.createBindings();
            scriptEngine.getContext().getBindings(ScriptContext.ENGINE_SCOPE).forEach(bindings::put);
            final TraversalSource clone = this.traversalSource.clone();
            clone.getStrategies().setTranslator(new StepTranslator());
            bindings.put(traversal.getStrategies().getTranslator().getAlias(), clone);
            final Traversal.Admin<?, ?> translatedTraversal = (Traversal.Admin<?, ?>) scriptEngine.eval(traversal.getStrategies().getTranslator().getTraversalScript(), bindings);
            assert !translatedTraversal.isLocked();
            assert !traversal.isLocked();
            traversal.setSideEffects(translatedTraversal.getSideEffects());
            traversal.setStrategies(traversal.getStrategies().clone().removeStrategies(TranslationStrategy.class));
            TraversalHelper.removeAllSteps(traversal);
            TraversalHelper.removeToTraversal((Step) translatedTraversal.getStartStep(), EmptyStep.instance(), traversal);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public TraversalSource getTraversalSource() {
        return this.traversalSource;
    }

    private void createAnonymousTraversalFunction() {
        if (null != this.anonymousTraversalClass) {
            final Function<? extends Traversal.Admin, ? extends Traversal.Admin> function = traversal -> {
                try {
                    TraversalStrategies clone = traversal.getStrategies().clone();
                    clone.setTranslator(this.traversalSource.getStrategies().getTranslator().getAnonymousTraversalTranslator());
                    traversal.setStrategies(clone);
                    return traversal;
                } catch (final Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            };
            try {
                this.anonymousTraversalClass.getMethod(TraversalSource.SET_ANONYMOUS_TRAVERSAL_FUNCTION, Function.class).invoke(null, function);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    private void destroyAnonymousTraversalFunction() {
        if (null != this.anonymousTraversalClass) {
            try {
                this.anonymousTraversalClass.getMethod(TraversalSource.SET_ANONYMOUS_TRAVERSAL_FUNCTION, Function.class).invoke(null, (Function) null);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
