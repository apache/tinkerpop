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
import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;
import org.apache.tinkerpop.gremlin.language.translator.GremlinTranslator;
import org.apache.tinkerpop.gremlin.language.translator.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsEqual;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.AssumptionViolatedException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsStringIgnoringCase;
import static org.hamcrest.core.StringEndsWith.endsWithIgnoringCase;
import static org.hamcrest.core.StringStartsWith.startsWithIgnoringCase;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@ScenarioScoped
public final class StepDefinition {

    private static final ObjectMapper mapper = new ObjectMapper();

    private World world;
    private GraphTraversalSource g;
    private final Map<String, Object> stringParameters = new HashMap<>();
    private Traversal traversal;
    private Object result;
    private Throwable error;
    private static final Pattern edgeTripletPattern = Pattern.compile("(.+)-(.+)->(.+)");
    private static final Pattern ioPattern = Pattern.compile("g\\.io\\(\"(.*)\"\\).*");
    private List<Pair<Pattern, Function<String,String>>> stringMatcherConverters = new ArrayList<Pair<Pattern, Function<String,String>>>() {{
        // expects json so that should port to the Gremlin script form
        add(Pair.with(Pattern.compile("m\\[(.*)\\]"), s -> {
            // can't handled embedded maps because of the string replace below on the curly braces
            final String[] items = s.replace("{", "").replace("}", "").split(",");
            final String listItems = Stream.of(items).map(String::trim).map(x -> {
                final String[] pair = x.split(":");

                // if wrapping double quotes aren't removed they end up re-wrapping again for pure string values
                // on conversion
                final String convertedKey = convertToString(pair[0].trim().replace("\"", ""));
                final String convertedValue = convertToString(pair[1].trim().replace("\"", ""));
                return String.format("%s:%s", convertedKey, convertedValue);
            }).collect(Collectors.joining(","));
            return String.format("[%s]", listItems);
        }));
        add(Pair.with(Pattern.compile("l\\[\\]"), s -> "[]"));
        add(Pair.with(Pattern.compile("l\\[(.*)\\]"), s -> {
            final String[] items = s.split(",");
            final String listItems = Stream.of(items).map(String::trim).map(x -> convertToString(x)).collect(Collectors.joining(","));
            return String.format("[%s]", listItems);
        }));
        add(Pair.with(Pattern.compile("s\\[\\]"), s -> "{}"));
        add(Pair.with(Pattern.compile("s\\[(.*)\\]"), s -> {
            final String[] items = s.split(",");
            final String setItems = Stream.of(items).map(String::trim).map(x -> convertToString(x)).collect(Collectors.joining(","));
            return String.format("{%s}", setItems);
        }));
        add(Pair.with(Pattern.compile("d\\[(NaN)\\]"), s -> "NaN"));
        add(Pair.with(Pattern.compile("d\\[(Infinity)\\]"), s -> "Infinity"));
        add(Pair.with(Pattern.compile("d\\[(-Infinity)\\]"), s -> "-Infinity"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.b"), s -> s + "b"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.s"), s -> s + "s"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.i"), s -> s + "i"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.l"), s -> s + "l"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.f"), s -> s + "f"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.d"), s -> s + "d"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.m"), s -> s + "m"));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.n"), s -> s + "n"));

        add(Pair.with(Pattern.compile("dt\\[(.*)\\]"), s -> String.format("datetime('%s')", s)));

        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.id"), s -> world.convertIdToScript(g.V().has("name", s).id().next(), Vertex.class)));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.sid"), s -> world.convertIdToScript(g.V().has("name", s).id().next(), Vertex.class)));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]"), s -> {
            final Iterator<Object> itty = g.V().has("name", s).id();
            return String.format("new Vertex(%s,\"%s\")", itty.hasNext() ?
                    world.convertIdToScript(itty.next(), Vertex.class) : s, Vertex.DEFAULT_LABEL);
        }));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.id"), s -> world.convertIdToScript(getEdgeId(g, s), Edge.class)));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.sid"), s -> world.convertIdToScript(getEdgeId(g, s), Edge.class)));
        add(Pair.with(Pattern.compile("t\\[(.*)\\]"), s -> String.format("T.%s", s)));
        add(Pair.with(Pattern.compile("D\\[(.*)\\]"), s -> String.format("Direction.%s", s)));
        add(Pair.with(Pattern.compile("M\\[(.*)\\]"), s -> String.format("Merge.%s", s)));

        // the following force ignore conditions as they cannot be parsed by the grammar at this time. the grammar will
        // need to be modified to handle them or perhaps these tests stay relegated to the JVM in some way for certain
        // cases like the lambda item which likely won't make it to the grammar as it's raw groovy.
        add(Pair.with(Pattern.compile("c\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a lambda as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Edge as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("p\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a Path as a parameter which is not supported by gremlin-language");
        }));
        add(Pair.with(Pattern.compile("(null)"), s -> "null"));
        add(Pair.with(Pattern.compile("(true)"), s -> "true"));
        add(Pair.with(Pattern.compile("(false)"), s -> "false"));
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

        add(Pair.with(Pattern.compile("l\\[\\]"), s -> new ArrayList<>()));
        add(Pair.with(Pattern.compile("l\\[(.*)\\]"), s -> {
            final String[] items = s.split(",");
            return Stream.of(items).map(String::trim).map(x -> convertToObject(x)).collect(Collectors.toList());
        }));

        add(Pair.with(Pattern.compile("s\\[\\]"), s -> new HashSet<>()));
        add(Pair.with(Pattern.compile("s\\[(.*)\\]"), s -> {
            final String[] items = s.split(",");
            return Stream.of(items).map(String::trim).map(x -> convertToObject(x)).collect(Collectors.toSet());
        }));

        // return the string values as is, used to wrap results that may contain other regex patterns
        add(Pair.with(Pattern.compile("str\\[(.*)\\]"), String::valueOf));

        /*
         * TODO FIXME Add same support for other languages (js, python, .net)
         */
        add(Pair.with(Pattern.compile("vp\\[(.+)\\]"), s -> getVertexProperty(g, s)));

        add(Pair.with(Pattern.compile("p\\[(.*)\\]"), s -> {
            final String[] items = s.split(",");
            Path path = ImmutablePath.make();
            final List<Object> pathObjects = Stream.of(items).map(String::trim).map(x -> convertToObject(x)).collect(Collectors.toList());
            for (Object o : pathObjects) {
                path = path.extend(o, Collections.emptySet());
            }
            return path;
        }));

        add(Pair.with(Pattern.compile("d\\[(NaN)\\]"), s -> Double.NaN));
        add(Pair.with(Pattern.compile("d\\[(Infinity)\\]"), s -> Double.POSITIVE_INFINITY));
        add(Pair.with(Pattern.compile("d\\[(-Infinity)\\]"), s -> Double.NEGATIVE_INFINITY));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.b"), Byte::parseByte));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.s"), Short::parseShort));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.i"), Integer::parseInt));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.l"), Long::parseLong));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.f"), Float::parseFloat));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.d"), Double::parseDouble));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.m"), BigDecimal::new));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.n"), BigInteger::new));

        add(Pair.with(Pattern.compile("dt\\[(.*)\\]"), s -> DatetimeHelper.parse(s)));

        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.id"), s -> g.V().has("name", s).id().next()));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]\\.sid"), s -> g.V().has("name", s).id().next().toString()));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]"), s -> detachVertex(g.V().has("name", s).next())));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.id"), s -> getEdgeId(g, s)));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]\\.sid"), s -> getEdgeIdString(g, s)));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]"), s -> getEdge(g, s)));

        add(Pair.with(Pattern.compile("t\\[(.*)\\]"), T::valueOf));
        add(Pair.with(Pattern.compile("D\\[(.*)\\]"), StepDefinition::getDirection));
        add(Pair.with(Pattern.compile("M\\[(.*)\\]"), Merge::valueOf));

        add(Pair.with(Pattern.compile("c\\[(.*)\\]"), s -> {
            throw new AssumptionViolatedException("This test uses a lambda as a parameter which is not supported by gremlin-language");
        }));

        add(Pair.with(Pattern.compile("(null)"), s -> null));
        add(Pair.with(Pattern.compile("(true)"), s -> true));
        add(Pair.with(Pattern.compile("(false)"), s -> false));
    }};

    /**
     * Some implementations of {@link Vertex} are not {@code Serializable} and may lead to test failures when passed as
     * parameters. One such instance is {@code HadoopVertex}. This method detaches vertices as tests should never
     * require implementation-specific elements.
     */
    private static DetachedVertex detachVertex(final Vertex vertex) {
        return new DetachedVertex(vertex.id(), vertex.label(),
                (List<VertexProperty>) IteratorUtils.asList(vertex.properties()).stream()
                        .map(vp -> detachVertexProperty((VertexProperty) vp)).collect(Collectors.toList()));
    }

    /**
     * Some implementations of {@link Edge} are not {@code Serializable} and may lead to test failures when passed as
     * parameters. One such instance is {@code HadoopEdge}. This method detaches edges as tests should never
     * require implementation-specific elements.
     */
    private static DetachedEdge detachEdge(final Edge edge) {
        return new DetachedEdge(edge.id(), edge.label(), IteratorUtils.asList(edge.properties()),
                edge.outVertex().id(), edge.outVertex().label(), edge.inVertex().id(), edge.inVertex().label());
    }

    /**
     * Some implementations of {@link VertexProperty} are not {@code Serializable} and may lead to test failures when passed as
     * parameters. One such instance is {@code HadoopVertexProperty}. This method detaches vertex properties as tests
     * should never require implementation-specific elements.
     */
    private static DetachedVertexProperty detachVertexProperty(final VertexProperty vp) {
        Map<String, Object> properties = new HashMap<>();
        vp.properties().forEachRemaining(p -> properties.put(((Property) p).key(), ((Property) p).value()));
        return new DetachedVertexProperty(vp.id(), vp.label(), vp.value(), properties, vp.element());
    }

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
        if (error != null) error = null;
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
        // when parameters are used literally, they are converted to string representations that can be recognized by
        // the grammar and parsed inline. when they are used as variables, they are converted to objects that can be
        // applied to the script as variables.
        if (world.useParametersLiterally())
            stringParameters.put(key, convertToString(value));
        else
            stringParameters.put(key, convertToObject(value));
    }

    @Given("the traversal of")
    public void theTraversalOf(final String docString) {
        try {
            final String rawGremlin = tryUpdateDataFilePath(docString);

            // when parameters are used literally, they are converted to string representations that can be recognized by
            // the grammar and parsed inline. when they are used as variables, they are converted to objects that can be
            // applied to the script as variables.
            final String gremlin = world.useParametersLiterally() ? applyParameters(rawGremlin) : rawGremlin;

            traversal = parseGremlin(gremlin);
        } catch (Exception ex) {
            ex.printStackTrace();
            error = ex;
        }
    }

    @When("iterated to list")
    public void iteratedToList() {
        try {
            result = traversal.toList();
        } catch (Exception ex) {
            error = ex;
        }
    }

    @When("iterated next")
    public void iteratedNext() {
        try {
            result = traversal.next();
        } catch (Exception ex) {
            error = ex;
        }
    }

    @Then("the result should be unordered")
    public void theResultShouldBeUnordered(final DataTable dataTable) {
        assertThatNoErrorWasThrown();

        final List<Object> actual = translateResultsToActual();

        // account for header in dataTable size
        assertEquals(dataTable.height() - 1, actual.size());

        // skip the header in the dataTable
        final Object[] expected = dataTable.asList().stream().skip(1).map(this::convertToObject).toArray();

        assertThat(actual, containsInAnyOrder(expected));
    }

    @Then("the result should be ordered")
    public void theResultShouldBeOrdered(final DataTable dataTable) {
        assertThatNoErrorWasThrown();

        final List<Object> actual = translateResultsToActual();

        // account for header in dataTable size
        assertEquals(dataTable.height() - 1, actual.size());

        // skip the header in the dataTable
        final Object[] expected = dataTable.asList().stream().skip(1).map(this::convertToObject).toArray();
        assertThat(actual, contains(expected));
    }

    @Then("the result should be of")
    public void theResultShouldBeOf(final DataTable dataTable) {
        assertThatNoErrorWasThrown();

        final List<Object> actual = translateResultsToActual();

        // skip the header in the dataTable
        final Object[] expected = dataTable.asList().stream().skip(1).map(this::convertToObject).toArray();
        assertThat(actual, everyItem(in(expected)));
    }

    @Then("the result should have a count of {int}")
    public void theResultShouldHaveACountOf(final Integer val) {
        assertThatNoErrorWasThrown();

        if (result instanceof Iterable)
            assertEquals(val.intValue(), IteratorUtils.count((Iterable) result));
        else if (result instanceof Map)
            assertEquals(val.intValue(), ((Map) result).size());
        else
            fail(String.format("Missing an assert for this type", result.getClass()));
    }

    @Then("the graph should return {int} for count of {string}")
    public void theGraphShouldReturnForCountOf(final Integer count, final String rawGremlin) {
        assertThatNoErrorWasThrown();

        final String gremlin = world.useParametersLiterally() ? applyParameters(rawGremlin) : rawGremlin;
        assertEquals(count.longValue(), ((GraphTraversal) parseGremlin(gremlin)).count().next());
    }

    @Then("the result should be empty")
    public void theResultShouldBeEmpty() {
        assertThatNoErrorWasThrown();

        assertThat(result, instanceOf(Collection.class));
        assertEquals(0, IteratorUtils.count((Collection) result));
    }

    @Then("the traversal will raise an error")
    public void theTraversalWillRaiseAnError() {
        assertNotNull(error);

        // consume the error now that it has been asserted
        error = null;
    }

    @Then("the traversal will raise an error with message {word} text of {string}")
    public void theTraversalWillRaiseAnErrorWithMessage(final String comparison, final String expectedMessage) {
        assertNotNull(error);

        // delegate error message assertion completely to the provider. if they choose to handle on their own then
        // skip the default assertions
        if (!world.handleErrorMessageAssertion(comparison, expectedMessage, error)) {
            switch (comparison) {
                case "containing":
                    assertThat(error.getMessage(), containsStringIgnoringCase(expectedMessage));
                    break;
                case "starting":
                    assertThat(error.getMessage(), startsWithIgnoringCase(expectedMessage));
                    break;
                case "ending":
                    assertThat(error.getMessage(), endsWithIgnoringCase(expectedMessage));
                    break;
                default:
                    throw new IllegalStateException(String.format(
                            "Unknown comparison of %s - must be one of: containing, starting or ending", comparison));
            }
        }

        // consume the error now that it has been asserted
        error = null;
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

    private void assertThatNoErrorWasThrown() {
        if (error != null) throw new RuntimeException(error);
    }

    private Traversal parseGremlin(final String script) {
        // tests the normalizer by running the script from the feature file first
        final String normalizedGremlin = GremlinTranslator.translate(script, Translator.LANGUAGE).getTranslated();

        // parse the Gremlin to a Traversal
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(normalizedGremlin));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        final GremlinParser.QueryContext ctx = parser.query();

        // when parameters are used literally, they are converted to string representations that can be recognized by
        // the grammar and parsed inline. when they are used as variables, they are converted to objects that can be
        // applied to the script as variables.
        if (world.useParametersLiterally())
            return (Traversal) new GremlinAntlrToJava(g).visitQuery(ctx);
        else
            return (Traversal) new GremlinAntlrToJava(g, new VariableResolver.DefaultVariableResolver(this.stringParameters)).visitQuery(ctx);
    }

    private List<Object> translateResultsToActual() {
        final List<Object> r = result instanceof List ? (List<Object>) result : IteratorUtils.asList(result);

        // gotta convert Map.Entry to individual Map coz that how we assert those for GLVs - dah
        final List<Object> actual = r.stream().map(o -> {
            if (o instanceof Map.Entry) {
                return new LinkedHashMap() {{
                    put(((Map.Entry<?, ?>) o).getKey(), ((Map.Entry<?, ?>) o).getValue());
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
            if (n.isNull()) {
                v = null;
            } else if (n.isArray()) {
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
                return converter.apply(matcher.groupCount() == 0 ? "" : matcher.group(1));
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
        return detachEdge(g.V().has("name", t.getValue0()).outE(t.getValue1()).toStream().
                   filter(edge -> g.V(edge.inVertex().id()).has("name", t.getValue2()).hasNext()).findFirst().get());
    }

    /**
     * Reuse edge triplet syntax for VertexProperty: vp[vertexName-key->value]
     */
    private VertexProperty getVertexProperty(final GraphTraversalSource g, final String e) {
        final Triplet<String,String,String> t = getEdgeTriplet(e);
        return (VertexProperty) g.V().has("name", t.getValue0())
                                     .properties(t.getValue1())
                                     .hasValue(convertToObject(t.getValue2()))
                .tryNext().orElse(null);
    }

    private static Object getEdgeId(final GraphTraversalSource g, final String e) {
        return getEdge(g, e).id();
    }

    private static String getEdgeIdString(final GraphTraversalSource g, final String e) {
        return getEdgeId(g, e).toString();
    }

    private String applyParameters(final String docString) {
        String replaced = docString;
        // sort from longest to shortest so that xx1 does not replace xx10
        final List<String> paramNames = new ArrayList<>(stringParameters.keySet());
        paramNames.sort((a,b) -> b.length() - a.length());
        for (String k : paramNames) {
            replaced = replaced.replace(k, stringParameters.get(k).toString());
        }
        return replaced;
    }

    private String tryUpdateDataFilePath(final String docString) {
        final Matcher matcher = ioPattern.matcher(docString);
        if (! matcher.matches()) { return docString; }
        final String relPath = matcher.group(1);
        final String absPath = world.changePathToDataFile(relPath);
        return docString.replace(relPath, escapeJava(absPath));
    }

    private static Direction getDirection(final String direction) {
        if (direction.equals("from")) return Direction.OUT;
        if (direction.equals("to")) return Direction.IN;
        return Direction.valueOf(direction);
    }

    /**
     * TinkerPop version of Hamcrest's {code containsInAnyOrder} that can use our custom assertions for {@link Path} and {@link Double}.
     */
    @SafeVarargs
    public static <T> org.hamcrest.Matcher<Iterable<? extends T>> containsInAnyOrder(final T... items) {
        return new IsIterableContainingInAnyOrder(getMatchers(items));
    }

    /**
     * TinkerPop version of Hamcrest's {code contains} that can use our custom assertions for {@link Path} and {@link Double}.
     */
    @SafeVarargs
    public static <T> org.hamcrest.Matcher<Iterable<? extends T>> contains(final T... items) {
        return new IsIterableContainingInOrder(getMatchers(items));
    }

    /**
     * TinkerPop version of Hamcrest's {code in} that can use our custom assertions for {@link Path}.
     */
    public static <T> org.hamcrest.Matcher<T> in(final Collection<T> collection) {
        return new IsInMatcher(collection);
    }

    /**
     * TinkerPop version of Hamcrest's {code in} that can use our custom assertions for {@link Path}.
     */
    public static <T> org.hamcrest.Matcher<T> in(final T[] elements) {
        return new IsInMatcher(elements);
    }

    private static <T> List<org.hamcrest.Matcher<? super T>> getMatchers(final T[] items) {
        final List<org.hamcrest.Matcher<? super T>> matchers = new ArrayList<>();

        for (int ix = 0; ix < items.length; ix++) {
            final T item = items[ix];

            // custom handle Path since we don't really want to assert the labels on the Path (i.e they aren't
            // part of the gherkin syntax)
            if (item instanceof Path) {
                matchers.add((org.hamcrest.Matcher<? super T>) new IsPathEqualToMatcher((Path) item));
            } else if (item instanceof Double && Double.isFinite((Double) item)) {
               // Allow for minor rounding errors with Double
               matchers.add((org.hamcrest.Matcher<? super T>) closeTo((Double) item, 0.000000000000001));
            } else {
                matchers.add(IsEqual.equalTo(item));
            }
        }
        return matchers;
    }

}
