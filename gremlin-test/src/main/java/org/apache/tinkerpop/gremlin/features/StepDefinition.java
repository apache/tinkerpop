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
package org.apache.tinkerpop.gremlin.features;

import com.google.inject.Inject;
import io.cucumber.datatable.DataTable;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.Scenario;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinAntlrToJava;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinLexer;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.AssumptionViolatedException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ScenarioScoped
public final class StepDefinition {

    private static final ObjectMapper mapper = new ObjectMapper();

    private World world;
    private GraphTraversalSource g;
    private final Map<String, String> stringParameters = new HashMap<>();
    private Traversal traversal;
    private Object result;
    private static final Pattern edgeTripletPattern = Pattern.compile("(.+)-(.+)->(.+)");
    private static final Pattern ioPattern = Pattern.compile("g\\.io\\(\"(.*)\"\\).*");
    private List<Pair<Pattern, Function<String,String>>> stringMatcherConverters = new ArrayList<Pair<Pattern, Function<String,String>>>() {{
        // expects json so that should port to the Gremlin script form - replace curly json braces with square ones
        // for Gremlin sake.
        add(Pair.with(Pattern.compile("m\\[(.*)\\]"), s -> s.replace('{','[').replace('}', ']')));

        add(Pair.with(Pattern.compile("l\\[\\]"), s -> "[]"));
        add(Pair.with(Pattern.compile("l\\[(.*)\\]"), s -> {
            final String[] items = s.split(",");
            final String listItems = Stream.of(items).map(String::trim).map(x -> convertToString(x)).collect(Collectors.joining(","));
            return String.format("[%s]", listItems);
        }));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.i"), s -> s));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.l"), s -> s + "l"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.f"), s -> s + "f"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.d"), s -> s + "d"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.m"), s -> String.format("new BigDecimal(%s)", s)));

        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.id"), s -> g.V().has("name", s).id().next().toString()));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.sid"), s -> g.V().has("name", s).id().next().toString()));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.id"), s -> getEdgeIdString(g, s)));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.sid"), s -> getEdgeIdString(g, s)));

        add(Pair.with(Pattern.compile("t\\[(.*)\\]"), s -> String.format("T.%s", s)));
        add(Pair.with(Pattern.compile("D\\[(.*)\\]"), s -> String.format("Direction.%s", s)));

        // the following force ignore conditions as they cannot be parsed by the grammar at this time. the grammar will
        // need to be modified to handle them or perhaps these tests stay relegated to the JVM in some way for certain
        // cases like the lambda item which likely won't make it to the grammar as it's raw groovy.
        add(Pair.with(Pattern.compile("c\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a lambda as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Vertex as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Edge as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("p\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Path as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("s\\[\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a empty Set as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("s\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Set as a parameter which is not supported by gremlin-language");
        }));
    }};

    private List<Pair<Pattern, Function<String,Object>>> objectMatcherConverters = new ArrayList<Pair<Pattern, Function<String,Object>>>() {{
        // expects json so that should port to the Gremlin script form - replace curly json braces with square ones
        // for Gremlin sake.
        add(Pair.with(Pattern.compile("m\\[(.*)\\]"), s -> {
            try {
                // read tree from JSON - can't parse right to Map as each m[] level needs to be managed individually
                return convertToObject(mapper.readTree(s));
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Can't parse JSON to map for %s", s), ex);
            }
        }));

        add(Pair.with(Pattern.compile("l\\[\\]"), s -> Collections.emptyList()));
        add(Pair.with(Pattern.compile("l\\[(.*)\\]"), s -> {
            final String[] items = s.split(",");
            return Stream.of(items).map(String::trim).map(x -> convertToObject(x)).collect(Collectors.toList());
        }));

        add(Pair.with(Pattern.compile("p\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Path as a parameter which is not supported by gremlin-language");
        }));

        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.i"), Integer::parseInt));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.l"), Long::parseLong));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.f"), Float::parseFloat));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.d"), Double::parseDouble));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.m"), BigDecimal::new));

        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.id"), s -> g.V().has("name", s).id().next()));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.sid"), s -> g.V().has("name", s).id().next().toString()));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]"), s -> g.V().has("name", s).next()));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.id"), s -> getEdgeId(g, s)));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.sid"), s -> getEdgeIdString(g, s)));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]"), s -> getEdge(g, s)));

        add(Pair.with(Pattern.compile("t\\[(.*)\\]"), T::valueOf));
        add(Pair.with(Pattern.compile("D\\[(.*)\\]"), Direction::valueOf));

        add(Pair.with(Pattern.compile("c\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a lambda as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("s\\[\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a empty Set as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("s\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Set as a parameter which is not supported by gremlin-language");
        }));

        add(Pair.with(Pattern.compile("(null)"), s -> null));
    }};

    @Inject
    public StepDefinition(final World world) {
        this.world = Objects.requireNonNull(world, "world must not be null");
    }

    @Before
    public void beforeEachScenario(final Scenario scenario) throws Exception {
        world.beforeEachScenario(scenario);
        stringParameters.clear();
        if (traversal != null) {
            traversal.close();
            traversal = null;
        }

        if (result != null) result = null;
    }

    @After
    public void afterEachScenario() throws Exception {
        world.afterEachScenario();
        if (g != null) g.close();
    }

    @Given("the {word} graph")
    public void givenTheXGraph(final String graphName) {
        if (graphName.equals("empty"))
            this.g = world.getGraphTraversalSource(null);
        else
            this.g = world.getGraphTraversalSource(GraphData.valueOf(graphName.toUpperCase()));
    }

    @Given("the graph initializer of")
    public void theGraphInitializerOf(final String gremlin) {
        parseGremlin(gremlin).iterate();
    }

    @Given("using the parameter {word} defined as {string}")
    public void usingTheParameterXDefinedAsX(final String key, final String value) {
        stringParameters.put(key, convertToString(value));
    }

    @Given("using the parameter {word} of P.{word}\\({string})")
    public void usingTheParameterXOfPX(final String key, final String pval, final String string) {
        stringParameters.put(key, String.format("P.%s(%s)", pval, convertToString(string)));
    }

    @Given("the traversal of")
    public void theTraversalOf(final String docString) {
        final String gremlin = tryUpdateDataFilePath(docString);
        traversal = parseGremlin(applyParameters(gremlin));
    }

    @When("iterated to list")
    public void iteratedToList() {
        result = traversal.toList();
    }

    @When("iterated next")
    public void iteratedNext() {
        result = traversal.next();
    }

    @Then("the result should be unordered")
    public void theResultShouldBeUnordered(final DataTable dataTable) {
        final List<Object> actual = translateResultsToActual();

        // account for header in dataTable size
        assertEquals(dataTable.height() - 1, actual.size());

        // skip the header in the dataTable
        final Object[] expected = dataTable.asList().stream().skip(1).map(this::convertToObject).toArray();
        assertThat(actual, containsInAnyOrder(expected));
    }

    @Then("the result should be ordered")
    public void theResultShouldBeOrdered(final DataTable dataTable) {
        final List<Object> actual = translateResultsToActual();

        // account for header in dataTable size
        assertEquals(dataTable.height() - 1, actual.size());

        // skip the header in the dataTable
        final Object[] expected = dataTable.asList().stream().skip(1).map(this::convertToObject).toArray();
        assertThat(actual, contains(expected));
    }

    @Then("the result should be of")
    public void theResultShouldBeOf(final DataTable dataTable) {
        final List<Object> actual = translateResultsToActual();

        // skip the header in the dataTable
        final Object[] expected = dataTable.asList().stream().skip(1).map(this::convertToObject).toArray();
        assertThat(actual, everyItem(in(expected)));
    }

    @Then("the result should have a count of {int}")
    public void theResultShouldHaveACountOf(final Integer val) {
        if (result instanceof Iterable)
            assertEquals(val.intValue(), IteratorUtils.count((Iterable) result));
        else if (result instanceof Map)
            assertEquals(val.intValue(), ((Map) result).size());
        else
            fail(String.format("Missing an assert for this type", result.getClass()));
    }

    @Then("the graph should return {int} for count of {string}")
    public void theGraphShouldReturnForCountOf(final Integer count, final String gremlin) {
        assertEquals(count.longValue(), ((GraphTraversal) parseGremlin(applyParameters(gremlin))).count().next());
    }

    @Then("the result should be empty")
    public void theResultShouldBeEmpty() {
        assertThat(result, instanceOf(Collection.class));
        assertEquals(0, IteratorUtils.count((Collection) result));
    }

    //////////////////////////////////////////////

    @Given("an unsupported test")
    public void anUnsupportedTest() {
        // placeholder text - no operation needed because it should be followed by nothingShouldHappenBecause()
    }

    @Then("nothing should happen because")
    public void nothingShouldHappenBecause(final String message) {
        throw new AssumptionViolatedException(String.format("This test is not supported by Gherkin because: %s", message));
    }

    //////////////////////////////////////////////

    private Traversal parseGremlin(final String script) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        final GremlinParser.QueryContext ctx = parser.query();
        return (Traversal) new GremlinAntlrToJava(g).visitQuery(ctx);
    }

    private List<Object> translateResultsToActual() {
        final List<Object> r = result instanceof List ? (List<Object>) result : IteratorUtils.asList(result);

        // gotta convert Map.Entry to individual Map coz that how we assert those for GLVs - dah
        final List<Object> actual = r.stream().map(o -> {
            if (o instanceof Map.Entry) {
                return new HashMap() {{
                    put(((Entry<?, ?>) o).getKey(), ((Entry<?, ?>) o).getValue());
                }};
            } else {
                return o;
            }
        }).collect(Collectors.toList());
        return actual;
    }

    private String convertToString(final String pvalue) {
        return convertToString(null, pvalue);
    }

    private String convertToString(final String pkey, final String pvalue) {
        for (Pair<Pattern,Function<String,String>> matcherConverter : stringMatcherConverters) {
            final Pattern pattern = matcherConverter.getValue0();
            final Matcher matcher = pattern.matcher(pvalue);
            if (matcher.find()) {
                final Function<String,String> converter = matcherConverter.getValue1();

                // when there are no groups there is a direct match
                return converter.apply(matcher.groupCount() == 0 ? "" : matcher.group(1));
            }
        }

        // this should be a raw string if it didn't match anything - suppose it could be a syntax error in the
        // test too, but i guess the test would fail so perhaps ok to just assume it's raw string value that
        // didn't need a transform by default
        return String.format("\"%s\"", pvalue);
    }

    private Object convertToObject(final Object pvalue) {
        final Object v;
        // we may get some json stuff if it's a m[]
        if (pvalue instanceof JsonNode) {
            final JsonNode n = (JsonNode) pvalue;
            if (n.isArray()) {
                v = IteratorUtils.stream(n.elements()).map(this::convertToObject).collect(Collectors.toList());
            } else if (n.isObject()) {
                final Map<Object,Object> m = new HashMap<>(n.size());
                n.fields().forEachRemaining(e -> m.put(convertToObject(e.getKey()), convertToObject(e.getValue())));
                v = m;
            } else if (n.isNumber()) {
                v = n.numberValue();
            } else if (n.isBoolean()) {
                v = n.booleanValue();
            } else {
                v = n.textValue();
            }
        } else {
            v = pvalue;
        }

        // if the object is already of a type then no need to push it through the matchers.
        if (!(v instanceof String)) return v;

        for (Pair<Pattern,Function<String,Object>> matcherConverter : objectMatcherConverters) {
            final Pattern pattern = matcherConverter.getValue0();
            final Matcher matcher = pattern.matcher((String) v);
            if (matcher.find()) {
                final Function<String,Object> converter = matcherConverter.getValue1();
                return converter.apply(matcher.group(1));
            }
        }

        // this should be a raw string if it didn't match anything - suppose it could be a syntax error in the
        // test too, but i guess the test would fail so perhaps ok to just assume it's raw string value that
        // didn't need a transform by default
        return String.format("%s", v);
    }

    private static Triplet<String,String,String> getEdgeTriplet(final String e) {
        final Matcher m = edgeTripletPattern.matcher(e);
        if (m.matches()) {
            return Triplet.with(m.group(1), m.group(2), m.group(3));
        }

        throw new IllegalStateException(String.format("Invalid edge identifier: %s", e));
    }

    private static Edge getEdge(final GraphTraversalSource g, final String e) {
        final Triplet<String,String,String> t = getEdgeTriplet(e);

        // make this OLAP proof since you can't leave the star graph
        return g.V().has("name", t.getValue0()).outE(t.getValue1()).toStream().
                   filter(edge -> g.V(edge.inVertex().id()).has("name", t.getValue2()).hasNext()).findFirst().get();
    }

    private static Object getEdgeId(final GraphTraversalSource g, final String e) {
        return getEdge(g, e).id();
    }

    private static String getEdgeIdString(final GraphTraversalSource g, final String e) {
        return getEdgeId(g, e).toString();
    }

    private String applyParameters(final String docString) {
        String replaced = docString;
        for (Map.Entry<String, String> kv : stringParameters.entrySet()) {
            replaced = replaced.replace(kv.getKey(), kv.getValue());
        }
        return replaced;
    }

    private String tryUpdateDataFilePath(final String docString) {
        final Matcher matcher = ioPattern.matcher(docString);
        final String gremlin = matcher.matches() ?
                docString.replace(matcher.group(1), world.changePathToDataFile(matcher.group(1))) : docString;
        return gremlin;
    }
}
