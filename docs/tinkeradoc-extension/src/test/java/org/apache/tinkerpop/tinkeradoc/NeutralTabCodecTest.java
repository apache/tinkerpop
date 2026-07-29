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
package org.apache.tinkerpop.tinkeradoc;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Round-trip tests for {@link NeutralTabCodec}. The codec is the on-the-wire form of the neutral
 * tab model, so lossless round-tripping is the correctness foundation for rendering both backends
 * from a single Gremlin execution.
 */
public class NeutralTabCodecTest {

    private static void assertRoundTrip(final List<NeutralTab> tabs) {
        final String json = NeutralTabCodec.serialize(tabs);
        final List<NeutralTab> parsed = NeutralTabCodec.parse(json);
        assertThat(parsed.size(), is(tabs.size()));
        for (int i = 0; i < tabs.size(); i++) {
            final NeutralTab a = tabs.get(i);
            final NeutralTab b = parsed.get(i);
            assertThat(b.getLabel(), is(a.getLabel()));
            assertThat(b.getLanguage(), is(a.getLanguage()));
            assertThat(b.getKind(), is(a.getKind()));
            assertThat(b.getContent(), is(a.getContent()));
        }
    }

    @Test
    public void shouldRoundTripEmptyList() {
        assertRoundTrip(Collections.emptyList());
        assertThat(NeutralTabCodec.serialize(Collections.emptyList()), is("[]"));
    }

    @Test
    public void shouldRoundTripConsolePlusSource() {
        assertRoundTrip(Arrays.asList(
                NeutralTab.console("groovy", "gremlin> g.V(1)\n==>v[1]"),
                NeutralTab.source("groovy", "g.V(1)")));
    }

    @Test
    public void shouldRoundTripMultiLanguageTabs() {
        assertRoundTrip(Arrays.asList(
                NeutralTab.console("groovy", "gremlin> g.V()"),
                NeutralTab.source("groovy", "g.V()"),
                NeutralTab.source("java", "g.V()"),
                NeutralTab.source("python", "g.V()")));
    }

    @Test
    public void shouldRoundTripContentWithQuotesAndBackslashes() {
        assertRoundTrip(Collections.singletonList(
                NeutralTab.source("groovy", "x = \"a \\\"quoted\\\" path C:\\\\tmp\"")));
    }

    @Test
    public void shouldRoundTripContentWithNewlinesTabsAndControlChars() {
        assertRoundTrip(Collections.singletonList(
                NeutralTab.console("groovy", "line1\n\tindented\r\nline3end")));
    }

    @Test
    public void shouldRoundTripContentWithCalloutMarkers() {
        assertRoundTrip(Collections.singletonList(
                NeutralTab.source("java", "g.V() <1>\ng.E() <2> <3>")));
    }

    @Test
    public void shouldRoundTripUnicodeContent() {
        assertRoundTrip(Collections.singletonList(
                NeutralTab.source("groovy", "name = 'Résumé — 日本語 ✓'")));
    }

    @Test
    public void shouldProduceExpectedJsonShape() {
        final String json = NeutralTabCodec.serialize(Collections.singletonList(
                NeutralTab.source("java", "g.V()")));
        assertThat(json, is("[{\"label\":\"java\",\"language\":\"java\",\"kind\":\"SOURCE\",\"content\":\"g.V()\"}]"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectMalformedJson() {
        NeutralTabCodec.parse("[{\"label\":\"x\"}]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonArray() {
        NeutralTabCodec.parse("{\"label\":\"x\"}");
    }
}
