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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineIntegrateTest extends AbstractGremlinTest {
    private static final Logger logger = LoggerFactory.getLogger(GremlinGroovyScriptEngineIntegrateTest.class);

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotBlowTheHeapParameterized() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final String[] gremlins = new String[]{
                "g.V(xxx).out().toList()",
                "g.V(xxx).in().toList()",
                "g.V(xxx).out().out().out().toList()",
                "g.V(xxx).out().groupCount()"
        };

        long parameterizedStartTime = System.currentTimeMillis();
        logger.info("Try to blow the heap with parameterized Gremlin.");
        try {
            for (int ix = 0; ix < 50001; ix++) {
                final Bindings bindings = engine.createBindings();
                bindings.put("g", g);
                bindings.put("xxx", graphProvider.convertId((ix % 4) + 1, Vertex.class));
                engine.eval(gremlins[ix % 4], bindings);

                if (ix > 0 && ix % 5000 == 0) {
                    logger.info(String.format("%s scripts processed in %s (ms) - rate %s (ms/q).", ix, System.currentTimeMillis() - parameterizedStartTime, Double.valueOf(System.currentTimeMillis() - parameterizedStartTime) / Double.valueOf(ix)));
                }
            }
        } catch (OutOfMemoryError oome) {
            fail("Blew the heap - the cache should prevent this from happening.");
        }
    }

    @Test
    public void shouldNotBlowTheHeapUnparameterized() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        long notParameterizedStartTime = System.currentTimeMillis();
        logger.info("Try to blow the heap with non-parameterized Gremlin.");
        try {
            for (int ix = 0; ix < 15001; ix++) {
                final Bindings bindings = engine.createBindings();
                engine.eval(String.format("1+%s", ix), bindings);
                if (ix > 0 && ix % 5000 == 0) {
                    logger.info(String.format("%s scripts processed in %s (ms) - rate %s (ms/q).", ix, System.currentTimeMillis() - notParameterizedStartTime, Double.valueOf(System.currentTimeMillis() - notParameterizedStartTime) / Double.valueOf(ix)));
                }
            }
        } catch (OutOfMemoryError oome) {
            fail("Blew the heap - the cache should prevent this from happening.");
        }
    }

}
