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
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
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

    private static final Pattern edgePattern = Pattern.compile(".*e\\d.*");

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
        add(Pair.with(Pattern.compile("s\\[\\]"), (k,v) -> "{}"));
        add(Pair.with(Pattern.compile("s\\[(.*)\\]"), (k,v) -> {
            final String[] items = v.split(",");
            final String setItems = Stream.of(items).map(String::trim).map(x -> String.format("\"%s\"", x)).collect(Collectors.joining(","));
            return String.format("{%s}", setItems);
        }));
        add(Pair.with(Pattern.compile("v\\[(.+)\\]"), (k,v) -> "\"1\""));
        add(Pair.with(Pattern.compile("v(\\d)"), (k,v) -> String.format("new Vertex(%s, \"vertex\")", v)));
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
    public static Iterable<String> queries() throws IOException {
        final Set<String> gremlins = new LinkedHashSet<>(DocumentationReader.parse(docsDir));
        gremlins.addAll(FeatureReader.parseGrouped(featureDir, stringMatcherConverters).values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        return gremlins;
    }

    @Parameterized.Parameter
    public String query;

    @Test
    public void test_parse() {
        // can't handle maps with complex embedded types at the moment - basically one Gremlin statement at this point
        assumeThat("Complex embedded types are not supported", query.contains("l[\"666\"]"), is(false));
        assumeThat("Lambdas are not supported", query.contains("Lambda.function("), is(false));
        // start of a closure
        assumeThat("Lambdas are not supported", query.contains("{"), is(false));
        assumeThat("withComputer() step is not supported", query.startsWith("g.withComputer("), is(false));
        assumeThat("Edge instances are not supported", edgePattern.matcher(query).matches(), is(false));
        assumeThat("fill() terminator is not supported", query.contains("fill("), is(false));
        assumeThat("program() is not supported", query.contains("program("), is(false));
        assumeThat("Casts are not supported", query.contains("(Map)"), is(false));

        parse(query);
    }
}
