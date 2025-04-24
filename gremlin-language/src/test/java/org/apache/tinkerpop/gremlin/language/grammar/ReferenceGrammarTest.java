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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.apache.tinkerpop.gremlin.language.corpus.DocumentationReader;
import org.apache.tinkerpop.gremlin.language.corpus.FeatureReader;
import org.apache.tinkerpop.gremlin.language.corpus.GrammarReader;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Builds a corpus of Gremlin statements from Recipes, "The Traversal" in Reference Documentation and the Gherkin
 * test suite and passes them all through the grammar parser.
 */
@RunWith(Parameterized.class)
public class ReferenceGrammarTest extends AbstractGrammarTest {
    private static final String featureDir = Paths.get("..", "gremlin-test", "src", "main", "resources", "org", "apache", "tinkerpop", "gremlin", "test", "features").toString();
    private static final String docsDir = Paths.get("..", "docs", "src").toString();
    private static final String grammarFile = Paths.get("src", "main", "antlr4","Gremlin.g4").toString();

    private static final Pattern lambdaPattern = Pattern.compile("g\\..*(branch|by|flatMap|map|filter|sack|sideEffect|fold\\(\\d*\\)|withSack)\\s*\\(?\\s*\\{.*");

    private static final List<Pair<Pattern, BiFunction<String,String,String>>> stringMatcherConverters = new ArrayList<Pair<Pattern, BiFunction<String,String,String>>>() {{
        add(Pair.with(Pattern.compile("m\\[(.*)\\]"), (k,v) -> {
            // can't handled embedded maps because of the string replace below on the curly braces
            final String[] items = v.replace("{", "").replace("}", "").split(",");
            final String listItems = Stream.of(items).map(String::trim).map(x -> {
                final String[] pair = x.split(":");
                final String convertedKey = String.format("%s", pair[0]);
                final String convertedValue = String.format("%s", pair[1]);
                return String.format("%s:%s", convertedKey, convertedValue);
            }).collect(Collectors.joining(","));
            return String.format("[%s]", listItems);
        }));
        add(Pair.with(Pattern.compile("l\\[\\]"), (k,v) -> "[]"));
        add(Pair.with(Pattern.compile("l\\[(.*)\\]"), (k,v) -> {
            final String[] items = v.split(",");
            final String listItems = Stream.of(items).map(String::trim).map(x -> String.format("\"%s\"", x)).collect(Collectors.joining(","));
            return String.format("[%s]", listItems);
        }));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]"), (k,v) -> "\"1\""));
        add(Pair.with(Pattern.compile("v(\\d)"), (k,v) -> String.format("Vertex(%s)", v)));
        add(Pair.with(Pattern.compile("e\\[(.+)\\]"), (k,v) -> "\"1\""));
        add(Pair.with(Pattern.compile("d\\[(.*)\\]\\.?.*"), (k,v) -> v));
        add(Pair.with(Pattern.compile("m\\[(.*)\\]"), (k,v) -> v.replace('{','[').replace('}', ']')));
        add(Pair.with(Pattern.compile("t\\[(.*)\\]"), (k,v) -> String.format("T.%s", v)));
        add(Pair.with(Pattern.compile("D\\[(.*)\\]"), (k,v) -> String.format("Direction.%s", v)));

        // the grammar doesn't support all the Gremlin we have in the gherkin set, so try to coerce it into
        // something that can be parsed so that we get maximum exercise over the parser itself.
        add(Pair.with(Pattern.compile("c\\[(.*)\\]"), (k,v) -> k.equals("c1") || k.equals("c2") ? "Order.desc" : "__.identity()"));  // closure -> Comparator || Traversal
        add(Pair.with(Pattern.compile("s\\[\\]"), (k,v) -> "[]"));  // set -> list
        add(Pair.with(Pattern.compile("s\\[(.*)\\]"), (k,v) -> "[]"));  // set -> list
        add(Pair.with(Pattern.compile("(null)"), (k,v) -> "null"));
        add(Pair.with(Pattern.compile("(true)"), (k,v) -> "true"));
        add(Pair.with(Pattern.compile("(false)"), (k,v) -> "false"));

    }};

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Pair<String, ParserRule>> parseTestItems() throws IOException {
        // gremlin scripts from docs
        final Set<Pair<String, ParserRule>> scripts = DocumentationReader.parse(docsDir).
                stream().map(g -> Pair.with(g, ParserRule.QUERY_LIST)).
                collect(Collectors.toCollection(LinkedHashSet::new));

        // helps make sure we're actually adding test scripts for each load. there are well over 500 gremlin
        // examples in the docs so we should have at least that much
        int size = scripts.size();
        assert size > 500;

        // gremlin scripts from feature tests
        scripts.addAll(FeatureReader.parseGrouped(featureDir, stringMatcherConverters).values().stream().
                flatMap(Collection::stream).
                map(g -> Pair.with(g, ParserRule.QUERY_LIST)).
                collect(Collectors.toList()));

        // there are well over 1000 tests so make sure we parsed something sensible
        assert size + 1000 < scripts.size();
        size = scripts.size();

        // validate that every keyword is parseable as a map key
        scripts.addAll(GrammarReader.parse(grammarFile).stream().
                flatMap(g -> Stream.of(
                        Pair.with(String.format("g.inject([%s:123])", g), ParserRule.QUERY_LIST)
                )).collect(Collectors.toList()));

        // there have to be at least 200 tokens parsed from the grammar. just picked a big number to help validate
        // that the GrammarReader is doing smart things.
        assert size + 200 < scripts.size();
        size = scripts.size();

        // tests for validating gremlin values like Map, String, etc.
        final InputStream stream = ReferenceGrammarTest.class.getClassLoader()
                .getResourceAsStream("gremlin-values.txt");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("#") && !line.isEmpty()) {
                    scripts.add(Pair.with(String.format("g.inject(%s)", line), ParserRule.QUERY_LIST));
                }
            }
        }

        // at least 300 lines in gremlin-values.txt
        assert size + 300 < scripts.size();

        return scripts;
    }

    @Parameterized.Parameter
    public Pair<String, ParserRule> parseTestItem;

    @Test
    public void test_parse() {
        final String script = parseTestItem.getValue0();

        if (parseTestItem.getValue1() == ParserRule.QUERY_LIST) {
            // can't handle maps with complex embedded types at the moment - basically one Gremlin statement at this point
            assumeThat("Complex embedded types are not supported", script.contains("l[\"666\"]"), is(false));
            assumeThat("Lambdas are not supported", script.contains("Lambda.function("), is(false));
            // start of a closure
            assumeThat("Lambdas are not supported", lambdaPattern.matcher(script).matches(), is(false));
            assumeThat("withComputer() step is not supported", script.startsWith("g.withComputer("), is(false));
            assumeThat("fill() terminator is not supported", script.contains("fill("), is(false));
            assumeThat("program() is not supported", script.contains("program("), is(false));
            assumeThat("Casts are not supported", script.contains("(Map)"), is(false));
        }

        parse(script, parseTestItem.getValue1());
    }
}
