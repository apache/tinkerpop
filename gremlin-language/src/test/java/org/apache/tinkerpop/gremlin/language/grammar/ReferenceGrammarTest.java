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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Builds a corpus of Gremlin statements from Recipes, "The Traversal" in Reference Documentation and the Gherkin
 * test suite and passes them all through the grammar parser.
 */
@RunWith(Parameterized.class)
public class ReferenceGrammarTest extends AbstractGrammarTest {

    private static final Pattern vertexPattern = Pattern.compile(".*v\\d.*");
    private static final Pattern edgePattern = Pattern.compile(".*e\\d.*");

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<String> queries() throws IOException {
        final Set<String> gremlins = new LinkedHashSet<>(DocumentationReader.parse("../"));
        gremlins.addAll(FeatureReader.parse("../"));
        return gremlins;
    }

    @Parameterized.Parameter
    public String query;

    @Test
    public void test_parse() {
        assumeThat("Lambdas are not supported", query.contains("l1"), is(false));
        assumeThat("Lambdas are not supported", query.contains("l2"), is(false));
        assumeThat("Lambdas are not supported", query.contains("pred1"), is(false));
        assumeThat("Lambdas are not supported", query.contains("c1"), is(false));
        assumeThat("Lambdas are not supported", query.contains("c2"), is(false));
        assumeThat("Lambdas are not supported", query.contains("Lambda.function("), is(false));
        // start of a closure
        assumeThat("Lambdas are not supported", query.contains("{"), is(false));
        assumeThat("withComputer() step is not supported", query.startsWith("g.withComputer("), is(false));
        assumeThat("Vertex instances are not supported", vertexPattern.matcher(query).matches(), is(false));
        assumeThat("Edge instances are not supported", edgePattern.matcher(query).matches(), is(false));
        assumeThat("fill() terminator is not supported", query.contains("fill("), is(false));
        assumeThat("withoutStrategies() is not supported", query.contains("withoutStrategies("), is(false));
        assumeThat("program() is not supported", query.contains("program("), is(false));

        parse(query);
    }
}
