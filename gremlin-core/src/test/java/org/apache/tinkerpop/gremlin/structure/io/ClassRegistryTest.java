/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache.getRegisteredStrategyClassByFullName;
import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache.unregisterStrategy;
import static org.apache.tinkerpop.gremlin.structure.io.ClassRegistry.lookup;
import static org.apache.tinkerpop.gremlin.structure.io.ClassRegistry.register;
import static org.apache.tinkerpop.gremlin.structure.io.ClassRegistry.unregister;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Covers what {@link ClassRegistry} holds, what it refuses, and how a name resolves against it. Also covers
 * {@link ClassRegistry#lookup(String)} falling back into {@code TraversalStrategies.GlobalCache}, which is how a
 * strategy the selector can construct is nameable without being registered here.
 */
public class ClassRegistryTest {

    @After
    public void unregisterProviderType() {
        unregister(ProviderType.class);
    }

    /**
     * The registry holds the classes that are not strategies, which is what it is for, so a registered one resolves by
     * its fully qualified name without being known to the strategy selector.
     */
    @Test
    public void shouldResolveRegisteredClassAsNameable() {
        register(ProviderType.class);

        assertEquals(ProviderType.class, lookup(ProviderType.class.getName()).get());
    }

    /**
     * Gryo names the two algorithms out of {@code MatchAlgorithmStrategy}'s {@code matchAlgorithmClass} field and the
     * two factories out of {@code HaltedTraverserStrategy}'s {@code haltedTraverserFactory} field, so the seeds are
     * what keep those readable. Nothing else here covers the seeds, only Gryo suites in another module.
     */
    @Test
    public void shouldSeedTheClassesGryoNamesFromStrategyFields() {
        for (final Class<?> clazz : Arrays.asList(MatchStep.GreedyMatchAlgorithm.class,
                MatchStep.CountMatchAlgorithm.class, DetachedFactory.class, ReferenceFactory.class)) {
            assertEquals("the seed for " + clazz.getName() + " is missing", clazz,
                    lookup(clazz.getName()).orElse(null));
        }
    }

    /**
     * A {@link TraversalStrategy} is registered with {@code GlobalCache}, which is what makes it nameable, so this
     * registry refuses one rather than holding a second place to register it. Both a strategy the selector knows and
     * one it does not are refused, since the class being a strategy is the whole of the test.
     */
    @Test
    public void shouldRefuseToRegisterAStrategy() {
        for (final Class<? extends TraversalStrategy> clazz : Arrays.asList(ReadOnlyStrategy.class,
                AbsentStrategy.class)) {
            try {
                register(clazz);
                fail("A TraversalStrategy must not be registered with ClassRegistry - " + clazz.getName());
            } catch (IllegalArgumentException ex) {
                assertThat(ex.getMessage(), containsString(clazz.getName()));
                assertThat(ex.getMessage(), containsString("TraversalStrategies.GlobalCache"));
            }
        }
    }

    /**
     * A caller unregistering a strategy here is making the same mistake as one registering it, since
     * {@code GlobalCache.unregisterStrategy} is what takes a strategy's nameability away.
     */
    @Test
    public void shouldRefuseToUnregisterAStrategy() {
        try {
            unregister(ReadOnlyStrategy.class);
            fail("A TraversalStrategy must not be unregistered from ClassRegistry");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString(ReadOnlyStrategy.class.getName()));
            assertThat(ex.getMessage(), containsString("TraversalStrategies.GlobalCache"));
        }

        assertEquals(ReadOnlyStrategy.class, lookup(ReadOnlyStrategy.class.getName()).get());
    }

    /**
     * A {@code null} reaching registration is a mistake in a caller's Java code rather than anything a serialized
     * traversal can send, so it fails with a message naming the parameter instead of the bare
     * {@link NullPointerException} that {@code isAssignableFrom} throws, which names nothing at all. Contrast
     * {@link #shouldNotResolveNullAsNameable}, where a {@code null} arrives on a read path and must not throw.
     */
    @Test
    public void shouldRefuseToRegisterNull() {
        try {
            register(null);
            fail("A null must not be registered with ClassRegistry");
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), containsString("clazz"));
        }
    }

    /**
     * Unregistration guards a {@code null} the same way, since both entry points reach the same field and a caller
     * passing one has made the same mistake in either direction.
     */
    @Test
    public void shouldRefuseToUnregisterNull() {
        try {
            unregister(null);
            fail("A null must not be unregistered from ClassRegistry");
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), containsString("clazz"));
        }
    }

    /**
     * The other direction of the containment: construction implies nameability, so a strategy registered for the
     * selector resolves through both accessors without being registered as nameable.
     */
    @Test
    public void shouldResolveConstructibleStrategyAsNameable() {
        assertEquals(ReadOnlyStrategy.class,
                getRegisteredStrategyClassByFullName(ReadOnlyStrategy.class.getName()).get());
        assertEquals(ReadOnlyStrategy.class,
                lookup(ReadOnlyStrategy.class.getName()).get());
    }

    @Test
    public void shouldNotResolveUnregisteredStrategyAsNameable() {
        unregisterStrategy(AbsentStrategy.class);
        assertFalse(lookup(AbsentStrategy.class.getName()).isPresent());
    }

    /**
     * A {@code null} must reach an empty {@code Optional} rather than a {@link NullPointerException}, since an
     * unchecked throw on a deserialization read path escapes the request handlers and leaves the client with no
     * response at all.
     */
    @Test
    public void shouldNotResolveNullAsNameable() {
        assertFalse(lookup(null).isPresent());
    }

    @Test
    public void shouldNotResolveSimpleNameAsNameable() {
        register(ProviderType.class);

        assertFalse(lookup(ReadOnlyStrategy.class.getSimpleName()).isPresent());
        assertFalse(lookup(ProviderType.class.getSimpleName()).isPresent());
    }

    /**
     * {@link ClassRegistry#lookup(String)} matches the exact {@link Class#getName()}, never assignability, so
     * registering a superclass admits that class alone. {@code Object} is the widest case there is: were the match on
     * assignability, one registration would make every class on the classpath nameable. Registered here rather than in
     * {@code unregisterProviderType} because no other test wants it, so it is undone in a {@code finally}.
     */
    @Test
    public void shouldNotResolveSubclassOfRegisteredClassAsNameable() {
        register(Object.class);
        try {
            assertEquals(Object.class, lookup(Object.class.getName()).orElse(null));

            assertFalse(lookup(ProviderType.class.getName()).isPresent());
            assertFalse(lookup(String.class.getName()).isPresent());
        } finally {
            unregister(Object.class);
        }
    }

    /**
     * The same exact match applies to an interface, so registering one does not admit the classes that implement it.
     * {@code ProviderType} implements {@code ProviderInterface} for this test, so the registration being no help to it
     * is the point rather than an accident of the two being unrelated.
     */
    @Test
    public void shouldNotResolveImplementorOfRegisteredInterfaceAsNameable() {
        register(ProviderInterface.class);
        try {
            assertEquals(ProviderInterface.class, lookup(ProviderInterface.class.getName()).orElse(null));

            assertFalse(lookup(ProviderType.class.getName()).isPresent());
        } finally {
            unregister(ProviderInterface.class);
        }
    }

    /**
     * Stands in for an interface a provider registers as nameable. {@code ProviderType} implements it so that
     * registering the interface can be shown not to admit the implementor.
     */
    private interface ProviderInterface {
    }

    /**
     * Stands in for a class a provider registers as nameable, which is deliberately not a {@link TraversalStrategy}
     * because the registry refuses one. It is unregistered again in {@code unregisterProviderType}.
     */
    private static final class ProviderType implements ProviderInterface {
    }

    /**
     * A {@link TraversalStrategy} that the selector does not know, so refusing to register it tests the class being a
     * strategy rather than the selector already holding it.
     */
    private static final class AbsentStrategy
            extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
            implements TraversalStrategy.DecorationStrategy {

        @Override
        public void apply(final Traversal.Admin<?, ?> traversal) {
            // do nothing
        }
    }
}
