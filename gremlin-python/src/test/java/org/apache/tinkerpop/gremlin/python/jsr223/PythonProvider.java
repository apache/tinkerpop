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

package org.apache.tinkerpop.gremlin.python.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.jsr223.ScriptEngineCache;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.AbstractTinkerGraphProvider;

import javax.script.ScriptException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonProvider extends AbstractTinkerGraphProvider {

    protected static final boolean IMPORT_STATICS = new Random().nextBoolean();

    static {
        JythonScriptEngineSetup.setup();
    }

    private static Set<String> SKIP_TESTS = new HashSet<>(Arrays.asList(
            "testProfileStrategyCallback",
            "testProfileStrategyCallbackSideEffect",
            "g_VX1X_out_injectXv2X_name",
            "shouldHidePartitionKeyForValues",
            "g_withSackXBigInteger_TEN_powX1000X_assignX_V_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack",
            //
            ProgramTest.Traversals.class.getCanonicalName(),
            TraversalInterruptionTest.class.getCanonicalName(),
            TraversalInterruptionComputerTest.class.getCanonicalName(),
            EventStrategyProcessTest.class.getCanonicalName(),
            ElementIdStrategyProcessTest.class.getCanonicalName()));

    @Override
    public Set<String> getSkipTests() {
        return SKIP_TESTS;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        if (isSkipTest(graph.configuration()))
            return graph.traversal();
        else {
            try {
                ScriptEngineCache.get("jython").eval(IMPORT_STATICS ?
                        "statics.load_statics(globals())" :
                        "statics.unload_statics(globals())");
            } catch (final ScriptException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            final GraphTraversalSource g = graph.traversal();
            return g.withStrategies(new TranslationStrategy(g, new PythonGraphSONJavaTranslator<>(PythonTranslator.of("g", IMPORT_STATICS), JavaTranslator.of(g))));
        }
    }

}
