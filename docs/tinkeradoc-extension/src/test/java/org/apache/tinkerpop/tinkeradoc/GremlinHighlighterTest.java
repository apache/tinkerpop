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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link GremlinHighlighter}. With a {@code null} parent node no JRuby runtime is
 * available, so {@code highlight()} falls back to HTML-escaped plain text; that path exercises the
 * callout stripping/re-injection logic without needing a live Asciidoctor context. The full
 * CodeRay-highlighted path is covered end-to-end by {@link IntegrationTest}.
 */
public class GremlinHighlighterTest {

    private final GremlinHighlighter highlighter = new GremlinHighlighter();

    @Test
    public void shouldReturnEmptyForNullOrEmptySource() {
        assertThat(highlighter.highlight(null, "groovy", null), is(""));
        assertThat(highlighter.highlight(null, "groovy", ""), is(""));
    }

    @Test
    public void shouldEscapeHtmlInFallbackPath() {
        final String result = highlighter.highlight(null, "java", "List<String> x = new ArrayList<>()");
        assertThat(result, containsString("List&lt;String&gt; x = new ArrayList&lt;&gt;()"));
    }

    @Test
    public void shouldReinjectTrailingCalloutAsConum() {
        final String result = highlighter.highlight(null, "groovy", "g.V() <1>");
        assertThat(result, containsString("<b class=\"conum\">(1)</b>"));
        // The raw callout marker must not survive in the output.
        assertThat(result, not(containsString("&lt;1&gt;")));
    }

    @Test
    public void shouldReinjectMultipleCalloutsOnSameLine() {
        final String result = highlighter.highlight(null, "groovy", "g.V() <1> <2>");
        assertThat(result, containsString("<b class=\"conum\">(1)</b>"));
        assertThat(result, containsString("<b class=\"conum\">(2)</b>"));
    }

    @Test
    public void shouldNotTreatMidLineAngleBracketsAsCallouts() {
        // Only trailing <n> markers are callouts; a mid-line comparison must be escaped, not injected.
        final String result = highlighter.highlight(null, "groovy", "x < 1");
        assertThat(result, containsString("x &lt; 1"));
        assertThat(result, not(containsString("conum")));
    }
}
