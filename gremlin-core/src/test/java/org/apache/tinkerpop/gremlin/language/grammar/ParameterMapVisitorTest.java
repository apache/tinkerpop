/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for parameter map parsing, validation, and security.
 */
public class ParameterMapVisitorTest {

    @Test
    public void shouldParseEmptyMap() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[:]");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void shouldParseNullInput() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void shouldParseEmptyStringInput() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void shouldParseSingleIntegerParameter() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"x\":1]");
        assertEquals(1, result.size());
        assertEquals(1, result.get("x"));
    }

    @Test
    public void shouldParseSingleStringParameter() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"name\":\"marko\"]");
        assertEquals(1, result.size());
        assertEquals("marko", result.get("name"));
    }

    @Test
    public void shouldParseSingleLongParameter() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"x\":1L]");
        assertEquals(1, result.size());
        assertEquals(1L, result.get("x"));
    }

    @Test
    public void shouldParseMultipleMixedParameters() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"x\":1,\"name\":\"marko\",\"flag\":true]");
        assertEquals(3, result.size());
        assertEquals(1, result.get("x"));
        assertEquals("marko", result.get("name"));
        assertEquals(true, result.get("flag"));
    }

    @Test
    public void shouldParseNullValue() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"x\":null]");
        assertEquals(1, result.size());
        assertNull(result.get("x"));
    }

    @Test
    public void shouldParseUuidValue() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"id\":UUID(\"bfa9bbe8-c3a3-4017-acc3-cd02dda55e3e\")]");
        assertEquals(1, result.size());
        assertEquals(UUID.fromString("bfa9bbe8-c3a3-4017-acc3-cd02dda55e3e"), result.get("id"));
    }

    @Test
    public void shouldParseNestedMapValue() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"m\":[\"name\":\"marko\"]]");
        assertEquals(1, result.size());
        assertTrue(result.get("m") instanceof Map);
        assertEquals("marko", ((Map<?, ?>) result.get("m")).get("name"));
    }

    @Test
    public void shouldParseListValue() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"x\":[1,2,3]]");
        assertEquals(1, result.size());
        assertTrue(result.get("x") instanceof List);
        assertEquals(3, ((List<?>) result.get("x")).size());
    }

    @Test
    public void shouldParseUnicodeKey() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"caf\\u00E9\":1]");
        assertEquals(1, result.size());
        assertEquals(1, result.get("caf\u00e9"));
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectMalformedInput() {
        GremlinQueryParser.parseParameters("[\"x\":");
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectNumericKey() {
        GremlinQueryParser.parseParameters("[1:\"value\"]");
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectEnumKey() {
        GremlinQueryParser.parseParameters("[(T.id):\"value\"]");
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectNonIdentifierStringKey() {
        GremlinQueryParser.parseParameters("[\"~id\":1]");
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectKeyWithSpaces() {
        GremlinQueryParser.parseParameters("[\"my key\":1]");
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectEmptyStringKey() {
        GremlinQueryParser.parseParameters("[\"\":1]");
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectExcessiveNestingDepth() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 35; i++) {
            sb.append("[\"a\":");
        }
        sb.append("1");
        for (int i = 0; i < 35; i++) {
            sb.append("]");
        }
        GremlinQueryParser.parseParameters(sb.toString());
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectNestedTraversalInValue() {
        GremlinQueryParser.parseParameters("[\"x\":__.out()]");
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectTerminatedTraversalInValue() {
        GremlinQueryParser.parseParameters("[\"x\":g.V().drop().iterate()]");
    }

    @Test
    public void shouldRejectTraversalInNestedList() {
        try {
            GremlinQueryParser.parseParameters("[\"x\":[__.out()]]");
            fail("Expected GremlinParserException for traversal in nested list");
        } catch (GremlinParserException e) {
            assertTrue(e.getMessage().contains("in parameter maps"));
        }
    }

    @Test
    public void shouldRejectTraversalInNestedSet() {
        try {
            GremlinQueryParser.parseParameters("[\"x\":{__.out()}]");
            fail("Expected GremlinParserException for traversal in nested set");
        } catch (GremlinParserException e) {
            assertTrue(e.getMessage().contains("in parameter maps"));
        }
    }

    @Test(expected = GremlinParserException.class)
    public void shouldRejectTraversalInNestedMapValue() {
        GremlinQueryParser.parseParameters("[\"x\":[\"a\":__.out()]]");
    }

    @Test
    public void shouldRejectTerminatedTraversalInNestedList() {
        try {
            GremlinQueryParser.parseParameters("[\"x\":[g.V().drop().iterate()]]");
            fail("Expected GremlinParserException for terminated traversal in nested list");
        } catch (GremlinParserException e) {
            assertTrue(e.getMessage().contains("in parameter maps"));
        }
    }

    @Test
    public void shouldRejectTerminatedTraversalInNestedSet() {
        try {
            GremlinQueryParser.parseParameters("[\"x\":{g.V().drop().iterate()}]");
            fail("Expected GremlinParserException for terminated traversal in nested set");
        } catch (GremlinParserException e) {
            assertTrue(e.getMessage().contains("in parameter maps"));
        }
    }

    @Test
    public void shouldRejectTraversalBuriedInListInsideList() {
        try {
            GremlinQueryParser.parseParameters("[\"x\":[1,[__.out()]]]");
            fail("Expected GremlinParserException for traversal buried in list inside list");
        } catch (GremlinParserException e) {
            assertTrue(e.getMessage().contains("in parameter maps"));
        }
    }

    @Test
    public void shouldRejectTerminatedTraversalInNestedMapValue() {
        try {
            GremlinQueryParser.parseParameters("[\"x\":[\"a\":g.V().drop().iterate()]]");
            fail("Expected GremlinParserException for terminated traversal in nested map value");
        } catch (GremlinParserException e) {
            assertTrue(e.getMessage().contains("in parameter maps"));
        }
    }

    @Test
    public void shouldAcceptStringContainingGremlinLikeContent() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"x\":\"g.V().drop()\"]");
        assertEquals(1, result.size());
        assertEquals("g.V().drop()", result.get("x"));
    }

    @Test
    public void shouldAcceptStringContainingTraversalSyntax() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[\"x\":\"__.out().count()\"]");
        assertEquals(1, result.size());
        assertEquals("__.out().count()", result.get("x"));
    }

    @Test
    public void shouldParseUnquotedIdentifierKey() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[x:1]");
        assertEquals(1, result.size());
        assertEquals(1, result.get("x"));
    }

    @Test
    public void shouldParseKeywordAsMapKey() {
        final Map<String, Object> result = GremlinQueryParser.parseParameters("[label:\"person\"]");
        assertEquals(1, result.size());
        assertEquals("person", result.get("label"));
    }
}
