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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_COMMIT;
import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_ROLLBACK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BytecodeHelperTest {
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();
    private static final List<String> MODULATING_OPERATORS = new ArrayList<String>() {{
        add(GraphTraversal.Symbols.to);
        add(GraphTraversal.Symbols.from);
        add(GraphTraversal.Symbols.read);
        add(GraphTraversal.Symbols.write);
        add(GraphTraversal.Symbols.emit);
        add(GraphTraversal.Symbols.until);
        add(GraphTraversal.Symbols.by);
        add(GraphTraversal.Symbols.with);
        add(GraphTraversal.Symbols.times);
        add(GraphTraversal.Symbols.as);
        add(GraphTraversal.Symbols.option);
    }};

    @Test
    public void shouldFindStrategy() {
        final Iterator<OptionsStrategy> itty = BytecodeHelper.findStrategies(g.with("x").with("y", 100).V().asAdmin().getBytecode(), OptionsStrategy.class);
        int counter = 0;
        while(itty.hasNext()) {
            final OptionsStrategy strategy = itty.next();
            if (strategy.getOptions().keySet().contains("x")) {
                assertThat(strategy.getOptions().get("x"), is(true));
                counter++;
            } else if (strategy.getOptions().keySet().contains("y")) {
                assertEquals(100, strategy.getOptions().get("y"));
                counter++;
            }
        }

        assertEquals(2, counter);
    }

    @Test
    public void shouldNotFindStrategy() {
        final Iterator<ReadOnlyStrategy> itty = BytecodeHelper.findStrategies(g.with("x").with("y", 100).V().asAdmin().getBytecode(), ReadOnlyStrategy.class);
        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldNotFindGremlinGroovyLambdaLanguage() {
        final Optional<String> lambda = BytecodeHelper.getLambdaLanguage(g.V().out("knows").map(Lambda.function("it.get()")).asAdmin().getBytecode());
        assertThat(lambda.isPresent(), is(true));
    }

    @Test
    public void shouldNotFindLambdaLanguage() {
        final Optional<String> lambda = BytecodeHelper.getLambdaLanguage(g.V().out("knows").map(__.identity()).asAdmin().getBytecode());
        assertThat(lambda.isPresent(), is(false));
    }

    @Test
    public void shouldRemoveBindings() {
        final Bindings b = Bindings.instance();
        final Bytecode bc = g.V(b.of("x", 1)).out(b.of("y", "knows")).asAdmin().getBytecode();
        final Bytecode filteredBeforeRemoved = BytecodeHelper.filterInstructions(
                bc, i -> Stream.of(i.getArguments()).anyMatch(o -> o instanceof Bytecode.Binding));
        assertEquals(2, filteredBeforeRemoved.getStepInstructions().size());

        BytecodeHelper.removeBindings(bc);
        final Bytecode filteredAfterRemoved = BytecodeHelper.filterInstructions(
                bc, i -> Stream.of(i.getArguments()).anyMatch(o -> o instanceof Bytecode.Binding));
        assertEquals(0, filteredAfterRemoved.getStepInstructions().size());
    }

    @Test
    public void shouldDetermineOperation() {
        assertThat(BytecodeHelper.isGraphOperation(TX_COMMIT.getBytecode()), is(true));
        assertThat(BytecodeHelper.isGraphOperation(TX_ROLLBACK.getBytecode()), is(true));
        assertThat(BytecodeHelper.isGraphOperation(g.V().out("knows").asAdmin().getBytecode()), is(false));
    }

    @Test
    public void findPossibleTraversalSteps() {
        Arrays.stream(Traversal.Symbols.class.getDeclaredFields()).filter(field -> {
            final int modifier = field.getModifiers();
            // We are interested in public static final string fields defined in the class
            return Modifier.isStatic(modifier) && Modifier.isPublic(modifier) && Modifier.isFinal(modifier);
        }).forEach(field -> {
            try {
                final List<Class<? extends Step>> steps = BytecodeHelper.findPossibleTraversalSteps((String) field.get(null));
                assertNotNull(steps);
            } catch (Exception ex) {
                Assert.fail(String.format("Error while finding possible Traversal step for operator %s. Exception: %s", field.getName(), ex));
            }
        });
    }

    @Test
    public void findPossibleTraversalStepsForModulatorOperators() {
        MODULATING_OPERATORS.forEach(operator -> {
            try {
                final List<Class<? extends Step>> steps = BytecodeHelper.findPossibleTraversalSteps(operator);
                assertNotNull(steps);
                assertThat(steps.size(), is(0));
            } catch (Exception ex) {
                Assert.fail(String.format("Error while finding possible Traversal step for operator %s. Exception: %s", operator, ex));
            }
        });
    }
}
