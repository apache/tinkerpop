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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TraversalStrategyVisitorTest {

    private GremlinAntlrToJava antlrToLanguage;

    @Parameterized.Parameter(value = 0)
    public String script;

    @Parameterized.Parameter(value = 1)
    public TraversalStrategy<?> expected;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {"ReadOnlyStrategy", ReadOnlyStrategy.instance()},
                {"new SeedStrategy(seed: 999999)", SeedStrategy.build().seed(999999).create()},
                {"SeedStrategy(seed: 999999)", SeedStrategy.build().seed(999999).create()},
                {"new PartitionStrategy(partitionKey: 'k', includeMetaProperties: true)", PartitionStrategy.build().partitionKey("k").includeMetaProperties(true).create()},
                {"new PartitionStrategy(partitionKey: 'k', writePartition: 'p', readPartitions: ['p','x','y'])", PartitionStrategy.build().partitionKey("k").writePartition("p").readPartitions("p", "x", "y").create()},
                {"ProductiveByStrategy", ProductiveByStrategy.instance()},
                {"new ProductiveByStrategy(productiveKeys: ['a','b'])", ProductiveByStrategy.build().productiveKeys("a", "b").create()},
                {"new EdgeLabelVerificationStrategy()", EdgeLabelVerificationStrategy.build().create()},
                {"EdgeLabelVerificationStrategy", EdgeLabelVerificationStrategy.build().create()},
                {"new EdgeLabelVerificationStrategy(logWarning: true, throwException: true)", EdgeLabelVerificationStrategy.build().logWarning(true).throwException(true).create()},
                {"new ReservedKeysVerificationStrategy()", ReservedKeysVerificationStrategy.build().create()},
                {"new ReservedKeysVerificationStrategy(logWarning: true, throwException: true)", ReservedKeysVerificationStrategy.build().logWarning(true).throwException(true).create()},
                {"ReservedKeysVerificationStrategy(logWarning: true, throwException: true)", ReservedKeysVerificationStrategy.build().logWarning(true).throwException(true).create()},
                {"new ReservedKeysVerificationStrategy(logWarning: true, throwException: false)", ReservedKeysVerificationStrategy.build().logWarning(true).create()},
                {"new ReservedKeysVerificationStrategy(keys: ['a','b'])", ReservedKeysVerificationStrategy.build().reservedKeys(new LinkedHashSet<>(Arrays.asList("a", "b"))).create()},
                {"new SubgraphStrategy(vertices: hasLabel('person'))", SubgraphStrategy.build().vertices(hasLabel("person")).create()},
                {"SubgraphStrategy(vertices: hasLabel('person'))", SubgraphStrategy.build().vertices(hasLabel("person")).create()},
                {"new SubgraphStrategy(vertices: hasLabel('person'), edges: hasLabel('knows'), vertexProperties: has('time', between(1234, 4321)), checkAdjacentVertices: true)", SubgraphStrategy.build().vertices(hasLabel("person")).edges(hasLabel("knows")).vertexProperties(has("time", P.between(1234, 4321))).checkAdjacentVertices(true).create()},
                {"CountStrategy", CountStrategy.instance()},
        });
    }

    @Before
    public void setup() throws Exception {
        antlrToLanguage = new GremlinAntlrToJava();
    }

    @Test
    public void shouldParseTraversalStrategy() {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        final GremlinParser.TraversalStrategyContext ctx = parser.traversalStrategy();
        final TraversalStrategy strategy = new TraversalStrategyVisitor(antlrToLanguage).visitTraversalStrategy(ctx);

        assertEquals(expected, strategy);
        assertEquals(ConfigurationConverter.getMap(expected.getConfiguration()),
                            ConfigurationConverter.getMap(strategy.getConfiguration()));
    }
}
