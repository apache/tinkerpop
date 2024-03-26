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
package org.apache.tinkerpop.gremlin.util.message;

import org.apache.tinkerpop.gremlin.util.Tokens;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RequestMessageV4Test {
    @Test
    public void shouldOverrideRequest() {
        final UUID request = UUID.randomUUID();
        final RequestMessageV4 msg = RequestMessageV4.build("x").overrideRequestId(request).create();
        assertEquals(request, msg.getRequestId());
    }

    @Test
    public void shouldSetScriptGremlin() {
        final String script = "g.V().both()";
        final RequestMessageV4 msg = RequestMessageV4.build(script).create();
        assertEquals(script, msg.getGremlin());
    }

    @Test
    public void shouldErrorSettingGremlinWithInvalidType() {
        final Integer script = 5;
        try {
            RequestMessageV4.build(script).create();
            fail("RequestMessage shouldn't accept Integer for gremlin input.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("gremlin argument for RequestMessage must be either String or Bytecode"));
        }
    }

    @Test
    public void shouldSetBindingsWithMap() {
        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("a", "b");
        bindings.put("g", "gmodern");
        final RequestMessageV4 msg = RequestMessageV4.build("gremlin").addBindings(bindings).create();
        assertEquals(bindings, msg.getArg(Tokens.ARGS_BINDINGS));
    }

    @Test
    public void shouldSetBindingsWithKeyValue() {
        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("a", "b");
        bindings.put("g", "gmodern");
        final RequestMessageV4 msg = RequestMessageV4.build("gremlin")
                .addBinding("a", "b")
                .addBinding("g", "gmodern")
                .create();
        assertEquals(bindings, msg.getArg(Tokens.ARGS_BINDINGS));
    }

    @Test
    public void shouldSetLanguage() {
        final String language = "fake-lang";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addLanguage(language).create();
        assertEquals(language, msg.getArg(Tokens.ARGS_LANGUAGE));
    }

    @Test
    public void shouldSetG() {
        final String g = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addG(g).create();
        assertEquals(g, msg.getArg(Tokens.ARGS_G));
    }

    @Test
    public void shouldGetFields() {
        final String g = "gmodern";
        final UUID rId = UUID.randomUUID();
        final String lang = "lang";
        final String query = "g.V()";
        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("b", "c");
        bindings.put("g", "gmodern");

        final RequestMessageV4 msg = RequestMessageV4.build(query).addG(g).addBindings(bindings).addLanguage(lang).overrideRequestId(rId).create();
        final Map<String, Object> fields = msg.getFields();
        assertEquals(g, fields.get(Tokens.ARGS_G));
        assertEquals(rId, fields.get(Tokens.REQUEST_ID));
        assertEquals(lang, fields.get(Tokens.ARGS_LANGUAGE));
        assertEquals(bindings, fields.get(Tokens.ARGS_BINDINGS));
        assertEquals(query, msg.getGremlin());
    }

    @Test
    public void shouldGetGAsArg() {
        final String g = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addG(g).create();
        assertEquals(g, msg.getArg(Tokens.ARGS_G));
    }

    @Test
    public void shouldGetGAsArgOrDefault() {
        final RequestMessageV4 msg = RequestMessageV4.build("g").create();
        assertEquals("b", msg.getArgOrDefault(Tokens.ARGS_G, "b"));
    }

    @Test
    public void shouldGetGAsArgAsOptional() {
        final String g = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addG(g).create();
        assertEquals(g, msg.optionalArgs(Tokens.ARGS_G).get());
    }

    @Test
    public void shouldNotBeAbleToGetGremlinQueryFromArgs() {
        final String query = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build(query).create();
        assertTrue(null == msg.getArg(Tokens.ARGS_GREMLIN));
    }
}
