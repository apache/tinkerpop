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

package org.apache.tinkerpop.gremlin.python;

import org.apache.tinkerpop.gremlin.groovy.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.creation.TranslationStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonTranslatorTest {

    static {
        JythonScriptEngineSetup.setup();
    }

    @Test
    public void shouldSupportStringSupplierLambdas() throws Exception {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal().withTranslator(GroovyTranslator.of("g"));
        GraphTraversal.Admin<Vertex, Number> t = (GraphTraversal.Admin) g.V().hasLabel("person").out().map(x -> "it.get().value('name').length()").asAdmin();
        System.out.println(t.getStrategies().getStrategy(TranslationStrategy.class).get().getTranslator().getTraversalScript());
        System.out.print(t.toList());
    }

}
