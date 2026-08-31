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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Direct tests for the canonical operator-list parser shared by {@link OlapClassLoadingPolicy} and
 * {@link OlapConfigKeyPolicy}. Pins the parsing contract here so a regression points at the parser rather than a
 * consumer.
 */
public class ConfigValueParserTest {

    @Test
    public void shouldParseCommaSeparatedString() {
        assertEquals(Arrays.asList("a.B", "c.D", "e.F"), new ArrayList<>(ConfigValueParser.parse("a.B,c.D,e.F")));
    }

    @Test
    public void shouldParseMultiValuedCollectionSplittingEachElementAndSkippingNulls() {
        assertEquals(Arrays.asList("a.B", "c.D", "e.F"),
                new ArrayList<>(ConfigValueParser.parse(Arrays.asList("a.B", null, "c.D,e.F"))));
    }

    @Test
    public void shouldTrimWhitespaceAndSkipBlankEntries() {
        assertEquals(Arrays.asList("a.B", "c.D"), new ArrayList<>(ConfigValueParser.parse(" a.B , , c.D ,")));
    }

    @Test
    public void shouldYieldNothingForNull() {
        assertTrue(ConfigValueParser.parse(null).isEmpty());
    }

    @Test
    public void shouldPreserveOrderAndDeduplicate() {
        assertEquals(Arrays.asList("a.B", "c.D", "e.F"),
                new ArrayList<>(ConfigValueParser.parse("a.B, c.D, a.B, e.F, c.D")));
    }
}
