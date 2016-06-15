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
    private final TraversalSource traversalSource;
    private final Class anonymousTraversalClass;


    public TranslationStrategy(final Translator translator, final TraversalSource traversalSource, final Class anonymousTraversalClass) {
        this.translator = translator;
        this.traversalSource = traversalSource;
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
            this.destroyAnonymousTraversalFunction();
            final Bindings bindings = new SimpleBindings();
            bindings.put(this.translator.getAlias(), this.traversalSource);
            Traversal.Admin<?, ?> translatedTraversal = (Traversal.Admin<?, ?>) engine.eval(traversalScriptString, bindings);
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

    @Override
    public void addSpawnStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        final TranslationStrategy clone = new TranslationStrategy(this.translator.clone(), this.traversalSource, this.anonymousTraversalClass);
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
        final TranslationStrategy clone = new TranslationStrategy(this.translator.clone(), this.traversalSource, this.anonymousTraversalClass);
        traversalSource.getStrategies().addStrategies(clone);
        clone.createAnonymousTraversalFunction();
        clone.translator.addSource(traversalSource, sourceName, arguments);
    }

    public Translator getTranslator() {
        return this.translator;
    }

    private void createAnonymousTraversalFunction() {
        if (null != this.anonymousTraversalClass) {
            final Function<? extends Traversal.Admin, ? extends Traversal.Admin> function = traversal -> {
                try {
                    traversal.setStrategies(traversal.getStrategies().clone().addStrategies(
                            new TranslationStrategy(this.translator.getAnonymousTraversalTranslator(),
                                    this.traversalSource,
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
