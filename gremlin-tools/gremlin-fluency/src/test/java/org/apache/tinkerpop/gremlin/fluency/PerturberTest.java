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
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PerturberTest {

    private static List<String> state(final AbstractTinkerGraph graph, final AbstractTinkerGraph anchor) {
        return new Canonical(anchor).state(graph);
    }

    @Test
    public void shouldCopyEverythingIncludingMetaAndMultiProperties() {
        final AbstractTinkerGraph crew = TinkerFactory.createTheCrew();
        final AbstractTinkerGraph copy = Perturber.copy(crew);
        assertThat(state(copy, crew), is(state(crew, crew)));
    }

    @Test
    public void shouldAddTheSameElementsForTheSameSeed() {
        final AbstractTinkerGraph modern = TinkerFactory.createModern();
        final AbstractTinkerGraph first = Perturber.copy(modern);
        final AbstractTinkerGraph second = Perturber.copy(modern);
        final AbstractTinkerGraph other = Perturber.copy(modern);
        Perturber.perturb(first, 7);
        Perturber.perturb(second, 7);
        Perturber.perturb(other, 8);

        // anchoring on the perturbed graph writes every element by id, so equal states mean equal ids too
        assertThat(state(second, first), is(state(first, first)));
        assertThat(state(other, first), is(not(state(first, first))));
    }

    @Test
    public void shouldOnlyAddToTheGraph() {
        final AbstractTinkerGraph modern = TinkerFactory.createModern();
        final AbstractTinkerGraph perturbed = Perturber.copy(modern);
        Perturber.perturb(perturbed, 1);

        assertThat(IteratorUtils.count(perturbed.vertices()), is(6L + Perturber.MIN_VERTICES));
        assertThat(IteratorUtils.count(perturbed.edges()), is(6L + Perturber.MIN_EDGES));
        assertThat(state(perturbed, modern), hasItems(state(modern, modern).toArray(new String[0])));
    }

    @Test
    public void shouldNeverConnectAVertexToItself() {
        for (long seed = 1; seed <= 50; seed++) {
            final AbstractTinkerGraph perturbed = Perturber.copy(TinkerFactory.createModern());
            Perturber.perturb(perturbed, seed);
            perturbed.edges().forEachRemaining(e ->
                    assertThat(e.toString(), e.outVertex().id().equals(e.inVertex().id()), is(false)));
        }
    }

    @Test
    public void shouldKeepAHierarchyFreeOfCyclesAndSecondParents() {
        final DomainSchema organization = DomainSchema.all().stream().
                filter(s -> s.getName().equals("organization")).findFirst().get();
        for (long seed = 1; seed <= 50; seed++) {
            final AbstractTinkerGraph graph = FluencyWorld.emptyGraph();
            final Vertex ceo = graph.addVertex(T.label, "employee", "name", "Lena");
            final Vertex manager = graph.addVertex(T.label, "employee", "name", "Omar");
            final Vertex engineer = graph.addVertex(T.label, "employee", "name", "Ines");
            manager.addEdge("reportsTo", ceo);
            engineer.addEdge("reportsTo", manager);

            Perturber.perturb(graph, seed);
            assertThat(organization.violations(graph).toString(), organization.violations(graph), is(empty()));
            graph.close();
        }
    }

    @Test
    public void shouldKeepEveryDomainScenarioToItsSchema() {
        // perturb() throws when its additions break the schema
        DomainSchemaTest.domainScenarios().keySet().forEach(scenario -> {
            for (long seed = 1; seed <= 3; seed++) {
                final AbstractTinkerGraph graph = DomainSchemaTest.initialGraph(scenario);
                Perturber.perturb(graph, seed);
                graph.close();
            }
        });
    }

    @Test
    public void shouldLeaveAnEmptyGraphAlone() {
        final AbstractTinkerGraph empty = FluencyWorld.emptyGraph();
        Perturber.perturb(empty, 1);
        assertThat(IteratorUtils.count(empty.vertices()), is(0L));
    }
}
