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

package org.apache.tinkerpop.gremlin.sparql;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrefixesTest {

    final static String TEST_QUERY = String.join(System.lineSeparator(), "SELECT *", "WHERE {}");
    final static String PREFIXED_QUERY = String.join(System.lineSeparator(),
            "PREFIX e: <" + Prefixes.BASE_URI + "edge#>",
            "PREFIX p: <" + Prefixes.BASE_URI + "property#>",
            "PREFIX v: <" + Prefixes.BASE_URI + "value#>",
            TEST_QUERY);

    @Test
    public void testGetURI() throws Exception {
        final String prefix = "test";
        final String uri = Prefixes.BASE_URI + prefix + "#";
        assertEquals(uri, Prefixes.getURI(prefix));
    }

    @Test
    public void testGetURIValue() throws Exception {
        final String prefix = "test";
        final String value = "value";
        final String uri = Prefixes.getURI(prefix) + value;
        assertEquals(value, Prefixes.getURIValue(uri));
    }

    @Test
    public void testGetPrefix() throws Exception {
        final String prefix = "test";
        final String uri = Prefixes.getURI(prefix);
        assertEquals(prefix, Prefixes.getPrefix(uri));
    }

    @Test
    public void testPrependString() throws Exception {
        assertEquals(PREFIXED_QUERY, Prefixes.prepend(TEST_QUERY));
    }

    @Test
    public void testPrependStringBuilder() throws Exception {
        final StringBuilder builder = new StringBuilder(TEST_QUERY);
        assertEquals(PREFIXED_QUERY, Prefixes.prepend(builder).toString());
    }
}