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
package org.apache.tinkerpop.gremlin.tinkergraph.jsr223;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.VariableResolverCustomizer;
import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerTextSearchFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

/**
 * Concrete testing of {@link GremlinLangScriptEngine} evaluations, particularly around its traversal cache.
 */
@RunWith(Parameterized.class)
public class TinkerGraphGremlinLangScriptEngineTest {

    @Parameterized.Parameter(value = 0)
    public String gremlinScript;

    @Parameterized.Parameter(value = 1)
    public List<Pair<Bindings, List<Object>>> bindingsAndResults;

    @Parameterized.Parameter(value = 2)
    public TinkerGraph graph;

    private GremlinLangScriptEngine scriptEngine;
    private GraphTraversalSource g;

    // Define vertex constants for the modern graph
    private static final Vertex V_MARKO = TinkerFactory.createModern().traversal().V(1).next();
    private static final Vertex V_VADAS = TinkerFactory.createModern().traversal().V(2).next();
    private static final Vertex V_LOP = TinkerFactory.createModern().traversal().V(3).next();
    private static final Vertex V_JOSH = TinkerFactory.createModern().traversal().V(4).next();
    private static final Vertex V_RIPPLE = TinkerFactory.createModern().traversal().V(5).next();
    private static final Vertex V_PETER = TinkerFactory.createModern().traversal().V(6).next();


    /**
     * Use {@link GValue} instance when resolving variables in the parser.
     */
    private final VariableResolverCustomizer variableResolverCustomizer = new VariableResolverCustomizer(
            VariableResolver.DefaultVariableResolver::new);

    /**
     * Enable caching
     */
    private final GremlinLangCustomizer gremlinLangCustomizer = new GremlinLangCustomizer(true, Caffeine.newBuilder());

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        final TinkerGraph tinkerSearchModern = TinkerFactory.createModern();
        tinkerSearchModern.getServiceRegistry().registerService(new TinkerTextSearchFactory(tinkerSearchModern));

        return Arrays.asList(new Object[][]{
                {
                    "g.V().limit(x).count()",
                    Arrays.asList(
                        Pair.of(createBindings("x", 1L), Arrays.asList(1L)),
                        Pair.of(createBindings("x", 2L), Arrays.asList(2L)),
                        Pair.of(createBindings("x", 4L), Arrays.asList(4L))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).values('age')",
                    Arrays.asList(
                        Pair.of(createBindings("x", "vadas"), Arrays.asList(27)),
                        Pair.of(createBindings("x", "marko"), Arrays.asList(29))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).out(y).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "josh", "y", "created"), Arrays.asList("lop" ,"ripple")),
                        Pair.of(createBindings("x", "marko", "y", "knows"), Arrays.asList("josh", "vadas"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).in(y).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "lop", "y", "created"), Arrays.asList("josh" ,"marko", "peter")),
                        Pair.of(createBindings("x", "vadas", "y", "knows"), Arrays.asList("marko"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).both(y,z).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "marko", "y", "created", "z", "knows"), Arrays.asList("josh" ,"lop", "vadas")),
                        Pair.of(createBindings("x", "vadas", "y", "knows", "z", "created"), Arrays.asList("marko"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('age', gt(x)).values('name')",
                    Arrays.asList(
                        Pair.of(createBindings("x", 31), Arrays.asList("josh" ,"peter")),
                        Pair.of(createBindings("x", 34), Arrays.asList("peter"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('age', gt(x).and(lt(y))).values('name')",
                    Arrays.asList(
                        Pair.of(createBindings("x", 26, "y", 30), Arrays.asList("marko" ,"vadas")),
                        Pair.of(createBindings("x", 26, "y", 29), Arrays.asList("vadas"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).union(both(y), both(z)).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "marko", "y", "created", "z", "knows"), Arrays.asList("josh" ,"lop", "vadas")),
                        Pair.of(createBindings("x", "vadas", "y", "knows", "z", "created"), Arrays.asList("marko"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.addV(x).label()",
                    Arrays.asList(
                        Pair.of(createBindings("x", "software"), Arrays.asList("software")),
                        Pair.of(createBindings("x", "person"), Arrays.asList("person"))
                    ),
                    TinkerGraph.open()
                },
                {
                    "g.addV().property(T.id, x).id()",
                    Arrays.asList(
                        Pair.of(createBindings("x", "abc"), Arrays.asList("abc")),
                        Pair.of(createBindings("x", "xyz"), Arrays.asList("xyz"))
                    ),
                    TinkerGraph.open(),
                },
                {
                    "g.V(vid).out().limit(xx1)",
                    Arrays.asList(
                        Pair.of(createBindings("vid", 1, "xx1", 2), Arrays.asList(V_LOP, V_VADAS)),
                        Pair.of(createBindings("vid", 4, "xx1", 1), Arrays.asList(V_RIPPLE)),
                        Pair.of(createBindings("vid", 6, "xx1", 3), Arrays.asList(V_LOP))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(1).out(\"knows\").outE(\"created\").range(xx1, xx2).inV()",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 0, "xx2", 1), Arrays.asList(V_RIPPLE)),
                        Pair.of(createBindings("xx1", 1, "xx2", 2), Arrays.asList(V_LOP)),
                        Pair.of(createBindings("xx1", 2, "xx2", 4), Collections.EMPTY_LIST)
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(1).out(\"knows\").out(\"created\").range(xx1, xx2)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 0, "xx2", 1), Arrays.asList(V_RIPPLE)),
                        Pair.of(createBindings("xx1", 0, "xx2", 2), Arrays.asList(V_RIPPLE, V_LOP)),
                        Pair.of(createBindings("xx1", 1, "xx2", 2), Arrays.asList(V_LOP))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(vid).out(\"created\").in(\"created\").range(xx1, xx2)",
                    Arrays.asList(
                        Pair.of(createBindings("vid", 1, "xx1", 1, "xx2", 3), Arrays.asList(V_JOSH, V_PETER)),
                        Pair.of(createBindings("vid", 4, "xx1", 0, "xx2", 2), Arrays.asList(V_JOSH, V_MARKO)),
                        Pair.of(createBindings("vid", 1, "xx1", 0, "xx2", 3), Arrays.asList(V_MARKO, V_JOSH, V_PETER))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(vid).out(\"created\").inE(\"created\").range(xx1, xx2).outV()",
                    Arrays.asList(
                        Pair.of(createBindings("vid", 1, "xx1", 1, "xx2", 3), Arrays.asList(V_JOSH, V_PETER)),
                        Pair.of(createBindings("vid", 4, "xx1", 0, "xx2", 2), Arrays.asList(V_JOSH, V_MARKO)),
                        Pair.of(createBindings("vid", 1, "xx1", 0, "xx2", 3), Arrays.asList(V_MARKO, V_JOSH, V_PETER))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().as(\"a\").in().as(\"b\").in().as(\"c\").select(\"a\",\"b\",\"c\").by(\"name\").limit(Scope.local, xx1)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 2), Arrays.asList(
                            Map.of("a", "lop", "b", "josh"),
                            Map.of("a", "ripple", "b", "josh")
                        )),
                        Pair.of(createBindings("xx1", 3), Arrays.asList(
                            Map.of("a", "lop", "b", "josh", "c", "marko"),
                            Map.of("a", "ripple", "b", "josh", "c", "marko")
                        )),
                        Pair.of(createBindings("xx1", 0), Arrays.asList(
                            Collections.EMPTY_MAP,
                            Collections.EMPTY_MAP
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().as(\"a\").out().as(\"b\").out().as(\"c\").select(\"a\",\"b\",\"c\").by(\"name\").range(Scope.local, xx1, xx2)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 1, "xx2", 3), Arrays.asList(
                                Map.of("b", "josh", "c", "ripple"),
                                Map.of("b", "josh", "c", "lop")
                        )),
                        Pair.of(createBindings("xx1", 0, "xx2", 3), Arrays.asList(
                            Map.of("a", "marko", "b", "josh", "c", "ripple"),
                            Map.of("a", "marko", "b", "josh", "c", "lop")
                        )),
                        Pair.of(createBindings("xx1", 1, "xx2", 1), Arrays.asList(
                            Collections.EMPTY_MAP,
                            Collections.EMPTY_MAP
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().hasLabel(\"person\").order().by(\"age\").skip(xx1).values(\"name\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 1), Arrays.asList("marko", "josh", "peter")),
                        Pair.of(createBindings("xx1", 0), Arrays.asList("vadas", "marko", "josh", "peter")),
                        Pair.of(createBindings("xx1", 3), Arrays.asList("peter"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().outE().values(\"weight\").fold().order(Scope.local).skip(Scope.local, xx1)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 2), Arrays.asList(Arrays.asList(0.4, 0.5, 1.0, 1.0))),
                        Pair.of(createBindings("xx1", 0), Arrays.asList(Arrays.asList(0.2, 0.4, 0.4, 0.5, 1.0, 1.0))),
                        Pair.of(createBindings("xx1", 4), Arrays.asList(Arrays.asList(1.0, 1.0)))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.addE(\"knows\").from(V(vid1)).to(V(vid2)).project(\"from\", \"to\").by(outV()).by(inV())",
                    Arrays.asList(
                        Pair.of(createBindings("vid1", V_MARKO.id(), "vid2", V_PETER.id()), Arrays.asList(
                                Map.of("from", V_MARKO, "to", V_PETER)
                        )),
                        Pair.of(createBindings("vid1", V_JOSH.id(), "vid2", V_MARKO.id()), Arrays.asList(
                                Map.of("from", V_JOSH, "to", V_MARKO)
                        )),
                        Pair.of(createBindings("vid1", V_PETER.id(), "vid2", V_JOSH.id()), Arrays.asList(
                                Map.of("from", V_PETER, "to", V_JOSH)
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.addE(xx1).from(V(vid1)).to(V(vid2)).property(\"weight\", xx2).project(\"label\", \"from\", \"to\", \"weight\").by(label).by(outV()).by(inV()).by(\"weight\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", "knows", "vid1", 1, "vid2", 6, "xx2", 0.1d), Arrays.asList(
                                Map.of("label", "knows", "from", V_MARKO, "to", V_PETER, "weight", 0.1d)
                        )),
                        Pair.of(createBindings("xx1", "created", "vid1", V_JOSH.id(), "vid2", V_MARKO.id(), "xx2", 0.2d), Arrays.asList(
                                Map.of("label", "created", "from", V_JOSH, "to", V_MARKO, "weight", 0.2d)
                        )),
                        Pair.of(createBindings("xx1", "knows", "vid1", V_PETER.id(), "vid2", V_JOSH.id(), "xx2", 0.3d), Arrays.asList(
                                Map.of("label", "knows", "from", V_PETER, "to", V_JOSH, "weight", 0.3d)
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(v1).addE(\"knows\").to(V(vid2)).project(\"from\", \"to\").by(outV()).by(inV())",
                    Arrays.asList(
                            Pair.of(createBindings("v1", V_MARKO, "vid2", V_PETER.id()), Arrays.asList(
                                    Map.of("from", V_MARKO, "to", V_PETER)
                            )),
                            Pair.of(createBindings("v1", V_JOSH, "vid2", V_MARKO.id()), Arrays.asList(
                                    Map.of("from", V_JOSH, "to", V_MARKO)
                            )),
                            Pair.of(createBindings("v1", V_PETER, "vid2", V_JOSH.id()), Arrays.asList(
                                    Map.of("from", V_PETER, "to", V_JOSH)
                            ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.E(eid)",
                    Arrays.asList(
                        Pair.of(createBindings("eid", 11), Arrays.asList(
                            TinkerFactory.createModern().traversal().E(11).next() //TODO cleanup
                        )),
                        Pair.of(createBindings("eid", 9), Arrays.asList(
                            TinkerFactory.createModern().traversal().E(9).next()
                        )),
                        Pair.of(createBindings("eid", 8), Arrays.asList(
                            TinkerFactory.createModern().traversal().E(8).next()
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.E(e7,e11)",
                    Arrays.asList(
                        Pair.of(createBindings(
                            "e7", TinkerFactory.createModern().traversal().E(7).next(),
                            "e11", TinkerFactory.createModern().traversal().E(11).next()
                        ), Arrays.asList(
                            TinkerFactory.createModern().traversal().E(7).next(),
                            TinkerFactory.createModern().traversal().E(11).next()
                        )),
                        Pair.of(createBindings(
                            "e7", TinkerFactory.createModern().traversal().E(8).next(),
                            "e11", TinkerFactory.createModern().traversal().E(9).next()
                        ), Arrays.asList(
                            TinkerFactory.createModern().traversal().E(8).next(),
                            TinkerFactory.createModern().traversal().E(9).next()
                        )),
                        Pair.of(createBindings(
                            "e7", TinkerFactory.createModern().traversal().E(10).next(),
                            "e11", TinkerFactory.createModern().traversal().E(12).next()
                        ), Arrays.asList(
                            TinkerFactory.createModern().traversal().E(10).next(),
                            TinkerFactory.createModern().traversal().E(12).next()
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(vid)",
                    Arrays.asList(
                        Pair.of(createBindings("vid", 1), Arrays.asList(V_MARKO)),
                        Pair.of(createBindings("vid", 4), Arrays.asList(V_JOSH)),
                        Pair.of(createBindings("vid", 6), Arrays.asList(V_PETER))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(vid).bothE().has(\"weight\", P.lt(1.0)).otherV()",
                    Arrays.asList(
                        Pair.of(createBindings("vid", 4), Arrays.asList(V_LOP)),
                        Pair.of(createBindings("vid", 1), Arrays.asList(V_LOP, V_VADAS)),
                        Pair.of(createBindings("vid", 6), Arrays.asList(V_LOP))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(xx1).values(\"name\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Arrays.asList(1, 2, 3)), Arrays.asList(
                            "marko", "vadas", "lop"
                        )),
                        Pair.of(createBindings("xx1", Arrays.asList(4, 5, 6)), Arrays.asList(
                            "josh", "ripple", "peter"
                        )),
                        Pair.of(createBindings("xx1", Arrays.asList(1, 4, 6)), Arrays.asList(
                            "marko", "josh", "peter"
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(xx1).values(\"name\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Arrays.asList(V_MARKO, V_VADAS, V_LOP)), Arrays.asList(
                            "marko", "vadas", "lop"
                        )),
                        Pair.of(createBindings("xx1", Arrays.asList(V_JOSH, V_RIPPLE, V_PETER)), Arrays.asList(
                            "josh", "ripple", "peter"
                        )),
                        Pair.of(createBindings("xx1", Arrays.asList(V_MARKO, V_JOSH, V_PETER)), Arrays.asList(
                            "marko", "josh", "peter"
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(1).out(xx1,xx2)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", "knows", "xx2", "created"), Arrays.asList(
                            V_VADAS, V_JOSH, V_LOP
                        )),
                        Pair.of(createBindings("xx1", "knows", "xx2", "other"), Arrays.asList(
                                V_VADAS, V_JOSH
                        )),
                        Pair.of(createBindings("xx1", null, "xx2", "created"), Arrays.asList(
                                V_LOP
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(vid1, vid2, vid3, vid4)",
                    Arrays.asList(
                        Pair.of(createBindings("vid1", 1, "vid2", 2, "vid3", 3, "vid4", 4), Arrays.asList(
                            V_MARKO, V_VADAS, V_LOP, V_JOSH
                        )),
                        Pair.of(createBindings("vid1", 2, "vid2", 3, "vid3", 4, "vid4", 5), Arrays.asList(
                            V_VADAS, V_LOP, V_JOSH, V_RIPPLE
                        )),
                        Pair.of(createBindings("vid1", 3, "vid2", 4, "vid3", 5, "vid4", 6), Arrays.asList(
                            V_LOP, V_JOSH, V_RIPPLE, V_PETER
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.inject(1,2).addV(xx1).property(\"age\", xx2).project(\"label\", \"age\").by(label).by(\"age\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", "animal", "xx2", 0), Arrays.asList(
                            Map.of("label", "animal", "age", 0),
                            Map.of("label", "animal", "age", 0)
                        )),
                        Pair.of(createBindings("xx1", "person", "xx2", 5), Arrays.asList(
                            Map.of("label", "person", "age", 5),
                            Map.of("label", "person", "age", 5)
                        )),
                        Pair.of(createBindings("xx1", "software", "xx2", 10), Arrays.asList(
                            Map.of("label", "software", "age", 10),
                            Map.of("label", "software", "age", 10)
                        ))
                    ),
                    TinkerGraph.open()
                },
                {
                    "g.addV(xx1).property(\"name\", xx2).project(\"label\", \"name\").by(label).by(\"name\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", "person", "xx2", "stephen"), Arrays.asList(
                            Map.of("label", "person", "name", "stephen")
                        )),
                        Pair.of(createBindings("xx1", "software", "xx2", "tinkergraph"), Arrays.asList(
                            Map.of("label", "software", "name", "tinkergraph")
                        )),
                        Pair.of(createBindings("xx1", "animal", "xx2", "puppy"), Arrays.asList(
                            Map.of("label", "animal", "name", "puppy")
                        ))
                    ),
                    TinkerGraph.open()
                },
                {
                    "g.addV().property(T.label, xx1).label()",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", "person"), Arrays.asList("person")),
                        Pair.of(createBindings("xx1", "software"), Arrays.asList("software")),
                        Pair.of(createBindings("xx1", "animal"), Arrays.asList("animal"))
                    ),
                    TinkerGraph.open()
                },
                {
                    "g.addV().property(T.id, xx1)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", "1"), Arrays.asList(new ReferenceVertex("1"))),
                        Pair.of(createBindings("xx1", "vertex-1"), Arrays.asList(new ReferenceVertex("vertex-1"))),
                        Pair.of(createBindings("xx1", "custom-id"), Arrays.asList(new ReferenceVertex("custom-id")))
                    ),
                    TinkerGraph.open()
                },
                {
                    "g.addE(xx1).property(\"weight\", 1).from(V().has(\"name\",\"marko\")).to(V().has(\"name\",\"vadas\")).label()",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", "knows"), Arrays.asList("knows")),
                        Pair.of(createBindings("xx1", "created"), Arrays.asList("created")),
                        Pair.of(createBindings("xx1", "likes"), Arrays.asList("likes"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().values(\"name\").order().tail(xx1)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 2), Arrays.asList("ripple", "vadas")),
                        Pair.of(createBindings("xx1", 1), Arrays.asList("vadas")),
                        Pair.of(createBindings("xx1", 3), Arrays.asList("peter", "ripple", "vadas"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().as(\"a\").out().as(\"b\").out().as(\"c\").select(\"a\",\"b\",\"c\").by(\"name\").tail(Scope.local, xx1)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 2), Arrays.asList(
                            Map.of("b", "josh", "c", "ripple"),
                            Map.of("b", "josh", "c", "lop")
                        )),
                        Pair.of(createBindings("xx1", 1), Arrays.asList(
                            Map.of("c", "ripple"),
                            Map.of("c", "lop")
                        )),
                        Pair.of(createBindings("xx1", 3), Arrays.asList(
                            Map.of("a", "marko", "b", "josh", "c", "ripple"),
                            Map.of("a", "marko", "b", "josh", "c", "lop")
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(vid1).values(\"age\").tail(Scope.local, 50)",
                    Arrays.asList(
                        Pair.of(createBindings("vid1", 1), Arrays.asList(29)),
                        Pair.of(createBindings("vid1", 2), Arrays.asList(27)),
                        Pair.of(createBindings("vid1", 4), Arrays.asList(32))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.mergeV(xx1).values(\"name\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Map.of("name", "stephen")), Arrays.asList("stephen")),
                        Pair.of(createBindings("xx1", Map.of("name", "marko")), Arrays.asList("marko")),
                        Pair.of(createBindings("xx1", Map.of("name", "alice")), Arrays.asList("alice"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.mergeV(xx1).option(Merge.onCreate, xx2).valueMap()",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Map.of("name", "stephen"), "xx2", Map.of("name", "stephen", "age", 19)), Arrays.asList(
                                Map.of("name", List.of("stephen"), "age", List.of(19))
                        )),
                        Pair.of(createBindings("xx1", Map.of("name", "alice"), "xx2", Map.of("name", "alice", "age", 25)), Arrays.asList(
                                Map.of("name", List.of("alice"), "age", List.of(25))
                        )),
                        Pair.of(createBindings("xx1", Map.of("name", "marko"), "xx2", Map.of("name", "marko", "age", 30)), Arrays.asList(
                                Map.of("name", List.of("marko"), "age", List.of(29))
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.mergeV(xx1).option(Merge.onMatch, xx2).valueMap()",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Map.of("name", "marko"),
                                              "xx2", Map.of("age", 19)), Arrays.asList(
                                Map.of("name", List.of("marko"), "age", List.of(19))
                        )),
                        Pair.of(createBindings("xx1", Map.of("name", "josh"),
                                              "xx2", Map.of("age", 35)), Arrays.asList(
                                Map.of("name", List.of("josh"), "age", List.of(35))
                        )),
                        Pair.of(createBindings("xx1", Map.of("name", "alice"),
                                              "xx2", Map.of("age", 25)), Arrays.asList(
                                Map.of("name", List.of("alice"))
                        ))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.mergeE(xx1).option(Merge.onCreate, xx2).values(\"created\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Map.of("weight", 0.8),
                                              "xx2", Map.of(Direction.from, V_MARKO, Direction.to, V_JOSH, "created", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 0.6),
                                              "xx2", Map.of(Direction.from, V_MARKO, Direction.to, V_JOSH, "created", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 0.5),
                                              "xx2", Map.of(Direction.from, V_MARKO, Direction.to, V_JOSH, "created", "Y")), Arrays.asList())
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.mergeE(xx1).option(Merge.onMatch, xx2).values(\"updated\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Map.of("weight", 0.5), "xx2", Map.of("updated", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 0.2), "xx2", Map.of("updated", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 1.0), "xx2", Map.of("updated", "Y")), Arrays.asList("Y", "Y"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(1).mergeE(xx1).option(Merge.onCreate, xx2).values(\"created\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Map.of("weight", 0.8),
                                              "xx2", Map.of(Direction.from, V_MARKO, Direction.to, V_JOSH, "created", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 0.6),
                                              "xx2", Map.of(Direction.from, V_MARKO, Direction.to, V_JOSH, "created", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 0.5),
                                              "xx2", Map.of(Direction.from, V_MARKO, Direction.to, V_JOSH, "created", "Y")), Arrays.asList())
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(1).mergeE(xx1).option(Merge.onMatch, xx2).values(\"updated\")",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", Map.of("weight", 0.5), "xx2", Map.of("updated", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 0.2), "xx2", Map.of("updated", "Y")), Arrays.asList("Y")),
                        Pair.of(createBindings("xx1", Map.of("weight", 1.0), "xx2", Map.of("updated", "Y")), Arrays.asList("Y", "Y"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.call(\"tinker.search\", xx2).element()",
                    Arrays.asList(
                        Pair.of(createBindings("xx2", Map.of("search", "mar")), Arrays.asList(V_MARKO)),
                        Pair.of(createBindings("xx2", Map.of("search", "vada")), Arrays.asList(V_VADAS)),
                        Pair.of(createBindings("xx2", Map.of("search", "josh")), Arrays.asList(V_JOSH))
                    ),
                    tinkerSearchModern
                },
                {
                    "g.V(xx1).out()",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 1), Arrays.asList(V_LOP, V_VADAS, V_JOSH)),
                        Pair.of(createBindings("xx1", 4), Arrays.asList(V_RIPPLE, V_LOP)),
                        Pair.of(createBindings("xx1", 6), Arrays.asList(V_LOP))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(xx1).out(xx2)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 1, "xx2", "knows"), Arrays.asList(V_VADAS, V_JOSH)),
                        Pair.of(createBindings("xx1", 1, "xx2", "created"), Arrays.asList(V_LOP)),
                        Pair.of(createBindings("xx1", 4, "xx2", "created"), Arrays.asList(V_RIPPLE, V_LOP))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(xx1).out(xx2, xx3)",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 1, "xx2", "knows", "xx3", "created"), Arrays.asList(V_VADAS, V_JOSH, V_LOP)),
                        Pair.of(createBindings("xx1", 4, "xx2", "created", "xx3", "knows"), Arrays.asList(V_RIPPLE, V_LOP)),
                        Pair.of(createBindings("xx1", 1, "xx2", "created", "xx3", "created"), Arrays.asList(V_LOP, V_LOP))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V(xx1).bothE(xx2).count()",
                    Arrays.asList(
                        Pair.of(createBindings("xx1", 1, "xx2", "knows"), Arrays.asList(2L)),
                        Pair.of(createBindings("xx1", 4, "xx2", "created"), Arrays.asList(2L)),
                        Pair.of(createBindings("xx1", 3, "xx2", "created"), Arrays.asList(3L))
                    ),
                    TinkerFactory.createModern()
                },
        });
    }

    private static Bindings createBindings(final Object... pairs) {
        final Bindings bindings = new SimpleBindings();
        final Map<String,Object> args = CollectionUtil.asMap(pairs);
        args.forEach(bindings::put);
        return bindings;
    }

    @Before
    public void setup() {
        // create a new engine each time to keep the cache clear
        scriptEngine = new GremlinLangScriptEngine(variableResolverCustomizer, gremlinLangCustomizer);

        // remove strategies to allow the tests to pass. the strategies replace placeholder steps with concrete ones
        // which remove the ability for the GValueReductionStrategy to update bindings in GValue from a cached
        // traversal. this is a bit of a limitation of the cache features right now. prior to this change it was
        // working because the TinkerGraphStepStrategy was only doing a replace on GraphStep instances and not on the
        // new GraphStepContract which would have also included the placeholder. if providers want the
        g = traversal().with(graph); // .withoutStrategies(TinkerGraphStepStrategy.class, TinkerGraphCountStrategy.class);
    }

    @Test
    public void shouldUseCacheForRepeatedScriptsWithVars() throws ScriptException {
        // store all traversal results to verify they are different instances
        final List<Object> results = Arrays.asList(new Object[bindingsAndResults.size()]);

        // execute the script with each set of bindings and store the results
        for (int i = 0; i < bindingsAndResults.size(); i++) {
            final Pair<Bindings, List<Object>> pair = bindingsAndResults.get(i);
            final Bindings bindings = pair.getLeft();
            final List<Object> expectedResults = pair.getRight();
            bindings.put("g", g);

            // execute the script
            final Object result = scriptEngine.eval(gremlinScript, bindings); // TODO::ensure cache was hit if i > 0
            assertThat(result, instanceOf(Traversal.Admin.class));

            // store the result for later comparison
            results.set(i, result);

            // verify the result matches the expected value
            for (Object expected : expectedResults) {
                assertEquals(expected, ((Traversal) result).next());
            }

            // verify that there are no unmatched results remaining
            assertThat("Traversal had more results than expected", ((Traversal) result).hasNext(), is(false));
        }

        // verify that each traversal instance is unique
        for (int i = 0; i < results.size(); i++) {
            for (int j = i + 1; j < results.size(); j++) {
                assertThat(results.get(i) != results.get(j), is(true));
            }
        }
    }
}
