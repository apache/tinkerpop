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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TranslationStrategy extends AbstractTraversalStrategy<TraversalStrategy.CreationStrategy> implements TraversalStrategy.CreationStrategy {

    private final Translator translator;
    private final Class anonymousTraversalClass;

    public TranslationStrategy(final Translator translator, final Class anonymousTraversalClass) {
        this.translator = translator;
        this.anonymousTraversalClass = anonymousTraversalClass;
        this.createAnonymousTraversalFunction();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))
            return;
        try {
            final String traversalScriptString = this.translator.getTraversalScript();
            ScriptEngine engine = ScriptEngineCache.get(this.translator.getScriptEngine());
            TraversalStrategies strategies = traversal.getStrategies().clone().removeStrategies(TranslationStrategy.class);
            this.destroyAnonymousTraversalFunction();
            final Bindings bindings = new SimpleBindings();
            bindings.put(this.translator.getAlias(), new GraphTraversalSource(traversal.getGraph().orElse(EmptyGraph.instance()), strategies));
            Traversal.Admin<?, ?> translatedTraversal = (Traversal.Admin<?, ?>) engine.eval(traversalScriptString, bindings);
            assert !translatedTraversal.isLocked();
            assert !traversal.isLocked();
            traversal.setSideEffects(translatedTraversal.getSideEffects());
            traversal.setStrategies(strategies);
            TraversalHelper.removeAllSteps(traversal);
            TraversalHelper.removeToTraversal((Step) translatedTraversal.getStartStep(), EmptyStep.instance(), traversal);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public void addSpawnStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        final TranslationStrategy clone = new TranslationStrategy(this.translator.clone(), this.anonymousTraversalClass);
        traversal.setStrategies(traversal.getStrategies().clone().addStrategies(clone));
        clone.createAnonymousTraversalFunction();
        clone.translator.addSpawnStep(traversal, stepName, arguments);
    }

    @Override
    public void addStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        this.translator.addStep(traversal, stepName, arguments);
    }

    @Override
    public void addSource(final TraversalSource traversalSource, final String sourceName, final Object... arguments) {
        final TranslationStrategy clone = new TranslationStrategy(this.translator.clone(), this.anonymousTraversalClass);
        traversalSource.getStrategies().addStrategies(clone);
        clone.createAnonymousTraversalFunction();
        clone.translator.addSource(traversalSource, sourceName, arguments);
    }

    public Translator getTranslator() {
        return this.translator;
    }

    private void createAnonymousTraversalFunction() {
        final Function<? extends Traversal.Admin, ? extends Traversal.Admin> function = traversal -> {
            try {
                traversal.setStrategies(traversal.getStrategies().clone().addStrategies(
                        new TranslationStrategy(this.translator.getAnonymousTraversalTranslator(),
                                this.anonymousTraversalClass)));
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

    private void destroyAnonymousTraversalFunction() {
        try {
            this.anonymousTraversalClass.getMethod(TraversalSource.SET_ANONYMOUS_TRAVERSAL_FUNCTION, Function.class).invoke(null, (Function) null);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
