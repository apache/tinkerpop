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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class EdgeLabelVerificationStrategyTest {
    private static final Translator<String,String> translator = GroovyTranslator.of("__");

    private final static Predicate<String> MSG_PREDICATE = Pattern.compile(
            "^The provided traversal contains a vertex step without any specified edge label: VertexStep.*")
            .asPredicate();

    private TestLogAppender logAppender;
    private Level previousLogLevel;

    @Before
    public void setupForEachTest() {
        final org.apache.log4j.Logger strategyLogger = org.apache.log4j.Logger.getLogger(AbstractWarningVerificationStrategy.class);
        previousLogLevel = strategyLogger.getLevel();
        strategyLogger.setLevel(Level.WARN);
        Logger.getRootLogger().addAppender(logAppender = new TestLogAppender());
    }

    @After
    public void teardownForEachTest() {
        final org.apache.log4j.Logger strategyLogger = org.apache.log4j.Logger.getLogger(AbstractWarningVerificationStrategy.class);
        strategyLogger.setLevel(previousLogLevel);
        Logger.getRootLogger().removeAppender(logAppender);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {__.inE(), false},
                {__.outE(), false},
                {__.bothE(), false},
                {__.to(Direction.OUT), false},
                {__.toE(Direction.IN), false},
                {__.inE("knows"), true},
                {__.outE("knows"), true},
                {__.bothE("created", "knows"), true},
                {__.to(Direction.OUT, "created", "knows"), true},
                {__.toE(Direction.IN, "knows"), true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin traversal;

    @Parameterized.Parameter(value = 1)
    public boolean allow;

    @Test
    public void shouldIgnore() {
        final String repr = translator.translate(traversal.getBytecode());
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(EdgeLabelVerificationStrategy.build().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
        assertThat(repr, logAppender.isEmpty());
    }

    @Test
    public void shouldOnlyThrow() {
        final String repr = translator.translate(traversal.getBytecode());
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(EdgeLabelVerificationStrategy.build().throwException().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow vertex steps with unspecified edge labels: " + repr);
            } catch (VerificationException ise) {
                assertThat(repr, MSG_PREDICATE.test(ise.getMessage()));
            }
        }
        assertThat(repr, logAppender.isEmpty());
    }

    @Test
    public void shouldOnlyLog() {
        final String repr = translator.translate(traversal.getBytecode());
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(EdgeLabelVerificationStrategy.build().logWarning().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
        if (!allow) {
            assertThat(String.format("Expected log entry not found in %s for %s", logAppender.messages, repr),
                    logAppender.messages().anyMatch(MSG_PREDICATE));
        }
    }

    @Test
    public void shouldThrowAndLog() {
        final String repr = translator.translate(traversal.getBytecode());
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(EdgeLabelVerificationStrategy.build().throwException().logWarning().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
            assertThat(repr, logAppender.isEmpty());
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow vertex steps with unspecified edge labels: " + repr);
            } catch (VerificationException ise) {
                assertThat(repr, MSG_PREDICATE.test(ise.getMessage()));
            }
            assertThat(String.format("Expected log entry not found in %s for %s", logAppender.messages, repr),
                    logAppender.messages().anyMatch(MSG_PREDICATE));
        }
    }

    class TestLogAppender extends AppenderSkeleton {

        private List<String> messages = new ArrayList<>();

        boolean isEmpty() {
            return messages.isEmpty();
        }

        Stream<String> messages() {
            return messages.stream();
        }

        @Override
        protected void append(org.apache.log4j.spi.LoggingEvent loggingEvent) {
            messages.add(loggingEvent.getMessage().toString());
        }

        @Override
        public void close() {

        }

        @Override
        public boolean requiresLayout() {
            return false;
        }
    }
}