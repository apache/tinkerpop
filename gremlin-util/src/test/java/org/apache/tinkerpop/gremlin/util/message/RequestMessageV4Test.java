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

import org.apache.tinkerpop.gremlin.util.TokensV4;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RequestMessageV4Test {
    @Test
    public void shouldSetScriptGremlin() {
        final String script = "g.V().both()";
        final RequestMessageV4 msg = RequestMessageV4.build(script).create();
        assertEquals(script, msg.getGremlin());
    }

    @Test
    public void shouldSetBindingsWithMap() {
        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("a", "b");
        bindings.put("g", "gmodern");
        final RequestMessageV4 msg = RequestMessageV4.build("gremlin").addBindings(bindings).create();
        assertEquals(bindings, msg.getField(TokensV4.ARGS_BINDINGS));
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
        assertEquals(bindings, msg.getField(TokensV4.ARGS_BINDINGS));
    }

    @Test
    public void shouldSetLanguage() {
        final String language = "fake-lang";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addLanguage(language).create();
        assertEquals(language, msg.getField(TokensV4.ARGS_LANGUAGE));
    }

    @Test
    public void shouldSetG() {
        final String g = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addG(g).create();
        assertEquals(g, msg.getField(TokensV4.ARGS_G));
    }

    @Test
    public void shouldSetTimeout() {
        final long timeout = 101L;
        final RequestMessageV4 msg = RequestMessageV4.build("g").addTimeoutMillis(timeout).create();
        assertEquals(timeout, (long) msg.getField(TokensV4.TIMEOUT_MS));
    }

    @Test
    public void shouldSetMaterializeProperties() {
        final RequestMessageV4 msgWithAll = RequestMessageV4.build("g").addMaterializeProperties(TokensV4.MATERIALIZE_PROPERTIES_ALL).create();
        assertEquals(TokensV4.MATERIALIZE_PROPERTIES_ALL, msgWithAll.getField(TokensV4.ARGS_MATERIALIZE_PROPERTIES));

        final RequestMessageV4 msgWithTokens = RequestMessageV4.build("g").addMaterializeProperties(TokensV4.MATERIALIZE_PROPERTIES_TOKENS).create();
        assertEquals(TokensV4.MATERIALIZE_PROPERTIES_TOKENS, msgWithTokens.getField(TokensV4.ARGS_MATERIALIZE_PROPERTIES));
    }

    @Test
    public void shouldErrorSettingMaterializePropertiesWithInvalidValue() {
        try {
            final RequestMessageV4 msgWithTokens = RequestMessageV4.build("g").addMaterializeProperties("notToken").create();
            fail("RequestMessage shouldn't accept notToken for materializeProperties.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("materializeProperties argument must be either token or all"));
        }
    }

    @Test
    public void shouldGetFields() {
        final String g = "gmodern";
        final String lang = "lang";
        final String query = "g.V()";
        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("b", "c");
        bindings.put("g", "gmodern");

        final RequestMessageV4 msg = RequestMessageV4.build(query).addG(g).addBindings(bindings).addLanguage(lang).create();
        final Map<String, Object> fields = msg.getFields();
        assertEquals(g, fields.get(TokensV4.ARGS_G));
        assertEquals(lang, fields.get(TokensV4.ARGS_LANGUAGE));
        assertEquals(bindings, fields.get(TokensV4.ARGS_BINDINGS));
        assertEquals(query, msg.getGremlin());
    }

    @Test
    public void shouldGetGAsArg() {
        final String g = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addG(g).create();
        assertEquals(g, msg.getField(TokensV4.ARGS_G));
    }

    @Test
    public void shouldGetGAsArgOrDefault() {
        final RequestMessageV4 msg = RequestMessageV4.build("g").create();
        assertEquals("b", msg.getFieldOrDefault(TokensV4.ARGS_G, "b"));
    }

    @Test
    public void shouldGetGAsArgAsOptional() {
        final String g = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build("g").addG(g).create();
        assertEquals(g, msg.optionalField(TokensV4.ARGS_G).get());
    }

    @Test
    public void shouldNotBeAbleToGetGremlinQueryFromArgs() {
        final String query = "gmodern";
        final RequestMessageV4 msg = RequestMessageV4.build(query).create();
        assertTrue(null == msg.getField(TokensV4.ARGS_GREMLIN));
    }

    @Test
    public void shouldNotContainRequestId() {
        final RequestMessageV4 msg = RequestMessageV4.build("g.V()").create();
        assertNull(msg.getField("requestId"));
    }
}
