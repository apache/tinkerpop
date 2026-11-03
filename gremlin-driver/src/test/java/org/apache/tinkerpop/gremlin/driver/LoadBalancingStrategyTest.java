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
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LoadBalancingStrategy.RoundRobin}. The {@code select} method ignores its
 * {@code RequestMessage} argument, so {@code null} is passed throughout.
 */
public class LoadBalancingStrategyTest {

    private static Host availableHost() {
        final Host host = mock(Host.class);
        when(host.isAvailable()).thenReturn(true);
        return host;
    }

    private static Host unavailableHost() {
        final Host host = mock(Host.class);
        when(host.isAvailable()).thenReturn(false);
        return host;
    }

    private static List<Host> drain(final Iterator<Host> itr) {
        final List<Host> hosts = new ArrayList<>();
        itr.forEachRemaining(hosts::add);
        return hosts;
    }

    /**
     * Forces the internal {@code index} counter to a known value so the index-related branches of
     * {@code select} (overflow reset and negative-modulo correction) can be exercised deterministically.
     */
    private static void setIndex(final LoadBalancingStrategy.RoundRobin strategy, final int value) throws Exception {
        final Field field = LoadBalancingStrategy.RoundRobin.class.getDeclaredField("index");
        field.setAccessible(true);
        ((AtomicInteger) field.get(strategy)).set(value);
    }

    private static int getIndex(final LoadBalancingStrategy.RoundRobin strategy) throws Exception {
        final Field field = LoadBalancingStrategy.RoundRobin.class.getDeclaredField("index");
        field.setAccessible(true);
        return ((AtomicInteger) field.get(strategy)).get();
    }

    @Test
    public void shouldSelectEachAvailableHostExactlyOncePerIterator() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        final Host h2 = availableHost();
        final Host h3 = availableHost();
        strategy.onNew(h1);
        strategy.onNew(h2);
        strategy.onNew(h3);

        final List<Host> selected = drain(strategy.select(null));

        assertEquals(3, selected.size());
        assertTrue(selected.containsAll(Arrays.asList(h1, h2, h3)));
        // each host appears exactly once
        assertEquals(1, selected.stream().filter(h -> h == h1).count());
        assertEquals(1, selected.stream().filter(h -> h == h2).count());
        assertEquals(1, selected.stream().filter(h -> h == h3).count());
    }

    @Test
    public void shouldSkipUnavailableHosts() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host up1 = availableHost();
        final Host down = unavailableHost();
        final Host up2 = availableHost();
        strategy.onNew(up1);
        strategy.onNew(down);
        strategy.onNew(up2);

        final List<Host> selected = drain(strategy.select(null));

        assertEquals(2, selected.size());
        assertTrue(selected.contains(up1));
        assertTrue(selected.contains(up2));
        assertFalse(selected.contains(down));
    }

    @Test
    public void shouldReturnEmptyIteratorWhenNoHostsAreAvailable() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        strategy.onNew(unavailableHost());
        strategy.onNew(unavailableHost());

        final Iterator<Host> itr = strategy.select(null);

        assertFalse(itr.hasNext());
        assertEquals(0, drain(itr).size());
    }

    @Test
    public void shouldAdvanceStartingHostOnConsecutiveSelects() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        final Host h2 = availableHost();
        final Host h3 = availableHost();
        strategy.onNew(h1);
        strategy.onNew(h2);
        strategy.onNew(h3);

        // The starting point rotates by one on each select, so the first host returned should differ
        // across consecutive calls and cycle through the full set.
        final Host first1 = strategy.select(null).next();
        final Host first2 = strategy.select(null).next();
        final Host first3 = strategy.select(null).next();

        assertEquals(3, new java.util.HashSet<>(Arrays.asList(first1, first2, first3)).size());
    }

    @Test
    public void shouldReflectHostAddedAndRemovedViaCallbacks() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        final Host h2 = availableHost();
        strategy.onNew(h1);
        strategy.onNew(h2);

        assertTrue(drain(strategy.select(null)).contains(h1));

        // onRemove delegates to onUnavailable which drops the host from the pool
        strategy.onRemove(h1);
        final List<Host> afterRemove = drain(strategy.select(null));
        assertFalse(afterRemove.contains(h1));
        assertTrue(afterRemove.contains(h2));
        assertEquals(1, afterRemove.size());

        // onNew delegates to onAvailable which adds a fresh host
        final Host h3 = availableHost();
        strategy.onNew(h3);
        final List<Host> afterAdd = drain(strategy.select(null));
        assertTrue(afterAdd.contains(h3));
        assertTrue(afterAdd.contains(h2));
        assertEquals(2, afterAdd.size());
    }

    @Test
    public void shouldNotAddDuplicateHostOnRepeatedCallbacks() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        // addIfAbsent semantics: adding the same host twice should not duplicate it
        strategy.onNew(h1);
        strategy.onAvailable(h1);

        final List<Host> selected = drain(strategy.select(null));
        assertEquals(1, selected.size());
        assertTrue(selected.contains(h1));
    }

    @Test
    public void shouldToggleHostViaAvailabilityCallbacks() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        final Host h2 = availableHost();
        strategy.onAvailable(h1);
        strategy.onAvailable(h2);
        assertEquals(2, drain(strategy.select(null)).size());

        strategy.onUnavailable(h2);
        final List<Host> selected = drain(strategy.select(null));
        assertEquals(1, selected.size());
        assertTrue(selected.contains(h1));
        assertFalse(selected.contains(h2));
    }

    @Test
    public void shouldInitializeWithSeededHostsAndSelectThemAll() {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        final Host h2 = availableHost();
        strategy.initialize(null, Arrays.asList(h1, h2));

        final List<Host> selected = drain(strategy.select(null));
        assertEquals(2, selected.size());
        assertTrue(selected.containsAll(Arrays.asList(h1, h2)));
    }

    @Test
    public void shouldInitializeWithEmptyHostsWithoutError() {
        // initialize uses Math.max(hosts.size(), 1) to seed the index, so an empty collection must not fail
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        strategy.initialize(null, new ArrayList<>());

        assertFalse(strategy.select(null).hasNext());
    }

    @Test
    public void shouldResetIndexWhenApproachingIntegerMaxValue() throws Exception {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        final Host h2 = availableHost();
        final Host h3 = availableHost();
        strategy.onNew(h1);
        strategy.onNew(h2);
        strategy.onNew(h3);

        // Just above the reset threshold (Integer.MAX_VALUE - 10000) so select() resets the counter to 0.
        setIndex(strategy, Integer.MAX_VALUE - 5000);

        drain(strategy.select(null));

        // the discriminating behavior: the counter was reset as part of the overflow guard
        assertEquals(0, getIndex(strategy));
    }

    @Test
    public void shouldCorrectNegativeModuloWhenIndexIsNegative() throws Exception {
        final LoadBalancingStrategy.RoundRobin strategy = new LoadBalancingStrategy.RoundRobin();
        final Host h1 = availableHost();
        final Host h2 = availableHost();
        final Host h3 = availableHost();
        strategy.onNew(h1);
        strategy.onNew(h2);
        strategy.onNew(h3);

        // A negative starting index drives the (c < 0) correction branch in the returned iterator.
        setIndex(strategy, -3);

        final List<Host> selected = drain(strategy.select(null));

        // -3, -2, -1 modulo three (after correction) map to 0, 1, 2 - every host exactly once
        assertEquals(3, selected.size());
        assertTrue(selected.containsAll(Arrays.asList(h1, h2, h3)));
    }
}
