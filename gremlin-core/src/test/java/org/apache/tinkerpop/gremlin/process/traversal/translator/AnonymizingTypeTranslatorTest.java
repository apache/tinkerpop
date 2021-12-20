/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.translator;

import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.language.grammar.NoOpTerminalVisitor;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

public class AnonymizingTypeTranslatorTest {
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());

    private void testAnonymize(final String query, final String expected) {
        try {
            final Bytecode bc = (Bytecode) GremlinQueryParser.parse(query, new NoOpTerminalVisitor());
            final Translator.ScriptTranslator translator = GroovyTranslator.of("g", new AnonymizingTypeTranslator());
            final String converted = translator.translate(bc).getScript();
            Assert.assertEquals(expected, converted);
        } catch (Exception ex) {
            throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    private void testAnonymize(final Traversal<?,?> t, final String expected) {
        final Translator.ScriptTranslator translator = GroovyTranslator.of("g", new AnonymizingTypeTranslator());
        final String converted = translator.translate(t).getScript();
        Assert.assertEquals(expected, converted);
    }

    @Test
    public void testBasicAnonymize() {

        Arrays.asList(
            
            new Pair<>("g.V().hasLabel('person')",
                       "g.V().hasLabel(string0)"),
            new Pair<>("g.V('3').valueMap(true).unfold().toList()",
                       "g.V(string0).valueMap(true).unfold().toList()"),
            new Pair<>("g.V().has('code','AUS').out().out().out().has('code','AGR').path().by('code').toList()",
                       "g.V().has(string0,string1).out().out().out().has(string0,string2).path().by(string0).toList()"),
            new Pair<>("g.V().has('length', 10L)",
                       "g.V().has(string0,long0)"),
            new Pair<>("g.V().hasId(between('1','3'))",
                       "g.V().hasId(P.gte(string0).and(P.lt(string1)))"),
            new Pair<>("g.V().hasId(between(1.0d,6.0d))",
                       "g.V().hasId(P.gte(double0).and(P.lt(double1)))"),
            new Pair<>("g.V().where(label().is(eq('airport'))).count().toList()",
                       "g.V().where(__.label().is(P.eq(string0))).count().toList()"),
            new Pair<>("g.V().has(label,'person').count()",
                       "g.V().has(T.label,string0).count()"),
            new Pair<>("g.V().has('runways',inside(1,3)).values('code','airport').toList()",
                       "g.V().has(string0,P.gt(integer0).and(P.lt(integer1))).values(string1,string2).toList()"),
            new Pair<>("g.V().hasId(within(100..115)).out().hasId(lte(46)).count().toList()",
                       "g.V().hasId(P.within([integer0, integer1, integer2, integer3, integer4, integer5, integer6, integer7, integer8, integer9, integer10, integer11, integer12, integer13, integer14, integer15])).out().hasId(P.lte(integer16)).count().toList()"),
            new Pair<>("g.V().out('nothing').tryNext()",
                       "g.V().out(string0).tryNext()"),
            new Pair<>("g.V().out('created').next(2)",
                       "g.V().out(string0).next(integer0)"),
            new Pair<>("g.V().out('created').toList()",
                       "g.V().out(string0).toList()"),
            new Pair<>("g.V().out('created').toSet()",
                       "g.V().out(string0).toSet()"),
            new Pair<>("g.V().out('created').explain()",
                       "g.V().out(string0).explain()"),
            new Pair<>("g.V().out('created').toBulkSet()",
                       "g.V().out(string0).toBulkSet()"),
            new Pair<>("g.V(1).property('city','santa fe').property('state','new mexico').valueMap()",
                       "g.V(integer0).property(string0,string1).property(string2,string3).valueMap()"),
            new Pair<>("g.V().values('name').order().by(Order.desc)",
                       "g.V().values(string0).order().by(Order.desc)"),
            new Pair<>("g.V(1).property(Cardinality.single, 'x', 'y')",
                       "g.V(integer0).property(VertexProperty.Cardinality.single,string0,string1)"),
            new Pair<>("g.V().as('x').out('created')",
                       "g.V().as(string0).out(string1)"),
            new Pair<>("g.V(3,4,5).aggregate('x').has('name','josh').as('a').select('x').unfold().hasLabel('software').addE('createdBy').to('a')",
                       "g.V(integer0,integer1,integer2).aggregate(string0).has(string1,string2).as(string3).select(string0).unfold().hasLabel(string4).addE(string5).to(string3)")

        ).forEach(test -> testAnonymize(test.getValue0(), test.getValue1()));

    }

    @Test
    public void shouldTranslateDate() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Date d = c.getTime();
        testAnonymize(g.inject(d), "g.inject(date0)");
    }

    @Test
    public void shouldTranslateTimestamp() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Timestamp t = new Timestamp(c.getTime().getTime());
        testAnonymize(g.inject(t), "g.inject(timestamp0)");
    }

    @Test
    public void shouldTranslateUuid() {
        final UUID uuid = UUID.fromString("ffffffff-fd49-1e4b-0000-00000d4b8a1d");
        testAnonymize(g.inject(uuid), "g.inject(uuid0)");
    }

}
