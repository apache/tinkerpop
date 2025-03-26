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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Parameterized.class)
public class AdjacentToIncidentStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;


    void applyAdjacentToIncidentStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(AdjacentToIncidentStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin();
        applyAdjacentToIncidentStrategy(original);
        assertEquals(repr, optimized, original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Traversal[][]{
                {__.outE().count(), __.outE().count()},
                {__.bothE("knows").count(), __.bothE("knows").count()},
                {__.properties().count(), __.properties().count()},
                {__.properties("name").count(), __.properties("name").count()},
                {__.out().count(), __.outE().count()},
                {__.in().count(), __.inE().count()},
                {__.both().count(), __.bothE().count()},
                {__.out("knows").count(), __.outE("knows").count()},
                {__.out("knows", "likes").count(), __.outE("knows", "likes").count()},
                {__.filter(__.out()), __.filter(__.outE())},
                {__.where(__.not(__.out())), __.where(__.not(__.outE()))},
                {__.where(__.out("knows")), __.where(__.outE("knows"))},
                {__.values().count(), __.properties().count()},
                {__.values("name").count(), __.properties("name").count()},
                {__.where(__.values()), __.where(__.properties())},
                {__.and(__.out(), __.in()), __.and(__.outE(), __.inE())},
                {__.or(__.out(), __.in()), __.or(__.outE(), __.inE())},
                {__.out().as("a").count(), __.outE().count()},   // TODO: is this good?
                {__.where(__.as("a").out("knows").as("b")), __.where(__.as("a").out("knows").as("b"))}});
    }
}