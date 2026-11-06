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

import org.apache.tinkerpop.gremlin.features.StepDefinition;
import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DomainSchemaTest {

    /**
     * The graph a scenario on the empty graph starts from, after its initializer.
     */
    static AbstractTinkerGraph initialGraph(final IntentScenario scenario) {
        final AbstractTinkerGraph graph = FluencyWorld.emptyGraph();
        for (ScenarioStep step : scenario.getSteps()) {
            if (step.getText().equals("the graph initializer of")) {
                final StepDefinition steps = new StepDefinition(graphData -> graph.traversal());
                steps.givenTheXGraph("empty");
                steps.theGraphInitializerOf(step.getDocString());
            }
        }
        return graph;
    }

    /**
     * The scenarios on the empty graph whose data follows one of the domain schemas, by the schema's name.
     */
    static Map<IntentScenario, DomainSchema> domainScenarios() {
        final Map<IntentScenario, DomainSchema> found = new LinkedHashMap<>();
        for (IntentScenario scenario : Corpus.fromClasspath().getScenarios()) {
            if (!"empty".equals(scenario.getGraph())) continue;
            final AbstractTinkerGraph graph = initialGraph(scenario);
            final DomainSchema schema = DomainSchema.forGraph(graph);
            if (null != schema) found.put(scenario, schema);
            graph.close();
        }
        return found;
    }

    @Test
    public void shouldReadEverySchemaWithTheShapeOfEachRelation() {
        assertThat(DomainSchema.all().stream().map(DomainSchema::getName).collect(Collectors.toList()),
                contains("contacts", "ledger", "organization", "telemetry"));

        final DomainSchema organization = schema("organization");
        assertThat(organization.getEdgeType("reportsTo").shape, is(DomainSchema.Shape.HIERARCHY));
        assertThat(organization.getEdgeType("owns").shape, is(DomainSchema.Shape.CONTAINMENT));
        assertThat(organization.getEdgeType("worksOn").shape, is(DomainSchema.Shape.FREE));
        assertThat(organization.getEdgeType("worksOn").keys.get("allocation").type, is(GType.DOUBLE));
        assertThat(schema("telemetry").getEdgeType("recorded").shape, is(DomainSchema.Shape.CONTAINMENT));
        assertThat(schema("ledger").getEdgeType("transfer").shape, is(DomainSchema.Shape.FREE));
        assertThat(schema("ledger").getEdgeType("transfer").keys.get("amount").type, is(GType.BIGDECIMAL));
    }

    @Test
    public void shouldFindNoSchemaForThePreloadedGraphs() {
        assertThat(DomainSchema.forGraph(TinkerFactory.createModern()), is(nullValue()));
        assertThat(DomainSchema.forGraph(FluencyWorld.emptyGraph()), is(nullValue()));
    }

    @Test
    public void shouldFindEveryDomainScenarioFollowingItsSchema() {
        final Map<IntentScenario, DomainSchema> scenarios = domainScenarios();
        assertThat(scenarios.size(), is(40));

        final List<String> violations = new ArrayList<>();
        scenarios.forEach((scenario, schema) -> {
            final AbstractTinkerGraph graph = initialGraph(scenario);
            schema.violations(graph).forEach(v -> violations.add(scenario.getName() + ": " + v));
            graph.close();
        });
        assertThat(violations.toString(), violations, is(empty()));
    }

    @Test
    public void shouldDescribeWhatTheSchemaRulesOut() {
        final AbstractTinkerGraph graph = FluencyWorld.emptyGraph();
        final Vertex lena = graph.addVertex("employee");
        lena.property("name", "Lena");
        final Vertex omar = graph.addVertex("employee");
        omar.property("name", "Omar");
        omar.property("grade", "A");
        omar.property("salary", 100);
        final Vertex ines = graph.addVertex("employee");
        ines.property("name", "Ines");
        lena.addEdge("reportsTo", omar);
        omar.addEdge("reportsTo", lena);
        ines.addEdge("reportsTo", lena);
        ines.addEdge("reportsTo", omar);
        ines.addEdge("memberOf", ines);

        final List<String> violations = schema("organization").violations(graph);
        assertThat(violations.toString(), violations, containsInAnyOrder(
                startsWith("vertex " + omar.id() + " has grade as a String, where the schema has CHAR"),
                startsWith("vertex " + omar.id() + " has the key salary, which the schema does not define"),
                endsWith("connects employee to employee, where the schema has employee to team"),
                endsWith("connects a vertex to itself"),
                startsWith("vertex " + ines.id() + " has 2 outgoing reportsTo edges"),
                startsWith("vertex " + lena.id() + " leads back to itself"),
                startsWith("vertex " + omar.id() + " leads back to itself")));
        graph.close();
    }

    private static DomainSchema schema(final String name) {
        return DomainSchema.all().stream().filter(s -> s.getName().equals(name)).findFirst().orElseThrow(
                () -> new AssertionError("No " + name + " schema"));
    }
}
