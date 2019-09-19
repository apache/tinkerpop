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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.T;
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class ReservedKeysVerificationStrategyTest {

    private final static Predicate<String> MSG_PREDICATE = Pattern.compile(
            ".*that is setting a property key to a reserved word.*")
            .asPredicate();

    private TestLogAppender logAppender;
    private Level previousLogLevel;

    @Before
    public void setupForEachTest() {
        final Logger strategyLogger = Logger.getLogger(AbstractWarningVerificationStrategy.class);
        previousLogLevel = strategyLogger.getLevel();
        strategyLogger.setLevel(Level.WARN);
        Logger.getRootLogger().addAppender(logAppender = new TestLogAppender());
    }

    @After
    public void teardownForEachTest() {
        final Logger strategyLogger = Logger.getLogger(AbstractWarningVerificationStrategy.class);
        strategyLogger.setLevel(previousLogLevel);
        Logger.getRootLogger().removeAppender(logAppender);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"__.addV().property('id',123)", __.addV().property("id", 123), false},
                {"__.addE('knows').property('id',123)", __.addE("knows").property("id", 123), false},
                {"__.addV().property(T.id,123)", __.addV().property(T.id, 123), true},
                {"__.addE('knows').property(T.label,123)", __.addE("knows").property(T.label, "blah"), true},
                {"__.addV().property('label','xyz')", __.addV().property("label", "xyz"), false},
                {"__.addE('knows').property('label','xyz')", __.addE("knows").property("id", "xyz"), false},
                {"__.addV().property('x','xyz', 'label', 'xxx')", __.addV().property("x", "xyz", "label", "xxx"), false},
                {"__.addV().property('x','xyz', 'not-Label', 'xxx')", __.addV().property("x", "xyz", "not-label", "xxx"), true},
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public boolean allow;

    @Test
    public void shouldIgnore() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ReservedKeysVerificationStrategy.build().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
        assertTrue(logAppender.isEmpty());
    }

    @Test
    public void shouldOnlyThrow() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ReservedKeysVerificationStrategy.build().throwException().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow vertex steps with unspecified edge labels: " + this.traversal);
            } catch (VerificationException ise) {
                assertTrue(MSG_PREDICATE.test(ise.getMessage()));
            }
        }
        assertTrue(logAppender.isEmpty());
    }

    @Test
    public void shouldOnlyLog() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ReservedKeysVerificationStrategy.build().logWarning().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
        if (!allow) {
            assertTrue(String.format("Expected log entry not found in %s", logAppender.messages),
                    logAppender.messages().anyMatch(MSG_PREDICATE));
        }
    }

    @Test
    public void shouldThrowAndLog() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ReservedKeysVerificationStrategy.build().throwException().logWarning().create());
        final Traversal traversal = this.traversal.asAdmin().clone();
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
            assertTrue(logAppender.isEmpty());
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow vertex steps with unspecified edge labels: " + this.traversal);
            } catch (VerificationException ise) {
                assertTrue(MSG_PREDICATE.test(ise.getMessage()));
            }
            assertTrue(String.format("Expected log entry not found in %s", logAppender.messages),
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