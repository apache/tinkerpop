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
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SackStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SideEffectStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TranslationStrategy extends AbstractTraversalStrategy<TraversalStrategy.CreationStrategy> implements TraversalStrategy.CreationStrategy {

    private Translator<?> translator;

    public TranslationStrategy(final Translator<?> translator) {
        this.translator = translator;
        if (!this.translator.getAlias().equals("__"))
            __.setAnonymousGraphTraversalSupplier(() -> (GraphTraversal) translator.__());
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))
            return;
        try {
            final String traversalScriptString = this.translator.getTraversalScript();
            System.out.println(traversalScriptString + "!!!");
            __.setAnonymousGraphTraversalSupplier(null);
            ScriptEngine engine = ScriptEngineCache.get(this.translator.getScriptEngine());
            TraversalStrategies strategies = traversal.getStrategies().clone().removeStrategies(TranslationStrategy.class);
            final Bindings bindings = new SimpleBindings();
            bindings.put(this.translator.getAlias(), new GraphTraversalSource(traversal.getGraph().orElse(EmptyGraph.instance()), strategies));
            Traversal.Admin<?, ?> translatedTraversal = (Traversal.Admin<?, ?>) engine.eval(traversalScriptString, bindings);
            assert !translatedTraversal.isLocked();
            assert !traversal.isLocked();
            //traversal.setSideEffects(translatedTraversal.getSideEffects());
            traversal.setStrategies(strategies);
            TraversalHelper.removeAllSteps(traversal);
            TraversalHelper.removeToTraversal((Step) translatedTraversal.getStartStep(), EmptyStep.instance(), traversal);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public void addStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        this.translator.addStep(traversal, stepName, arguments);
    }

    @Override
    public void addSource(final TraversalSource traversalSource, final String sourceName, final Object... arguments) {
        final TranslationStrategy clone = new TranslationStrategy(this.translator.clone());
        traversalSource.getStrategies().addStrategies(clone);
        clone.translator.addSource(traversalSource, sourceName, arguments);
    }

    public Translator getTranslator() {
        return this.translator;
    }
}
