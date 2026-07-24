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

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link HtmlTabRenderer} and the {@link NeutralTab} model. A {@code null} parent
 * node yields no JRuby runtime, so highlighting falls back to HTML-escaped plain text; that is
 * enough to verify the renderer produces the same tab-widget structure, labels, and kinds the
 * treeprocessor produced when it built {@link TabbedHtmlBuilder.Tab}s directly. The full
 * CodeRay-highlighted path is covered end-to-end by {@link IntegrationTest}.
 */
public class HtmlTabRendererTest {

    private HtmlTabRenderer renderer;

    @Before
    public void setUp() {
        renderer = new HtmlTabRenderer(new GremlinHighlighter(), new TabbedHtmlBuilder());
    }

    @Test
    public void consoleFactorySetsLabelLanguageAndKind() {
        final NeutralTab tab = NeutralTab.console("groovy", "gremlin> g.V()");
        assertThat(tab.getLabel(), is("console (groovy)"));
        assertThat(tab.getLanguage(), is("groovy"));
        assertThat(tab.getKind(), is(NeutralTab.Kind.CONSOLE));
        assertThat(tab.getContent(), is("gremlin> g.V()"));
    }

    @Test
    public void sourceFactorySetsLabelLanguageAndKind() {
        final NeutralTab tab = NeutralTab.source("python", "g.V()");
        assertThat(tab.getLabel(), is("python"));
        assertThat(tab.getLanguage(), is("python"));
        assertThat(tab.getKind(), is(NeutralTab.Kind.SOURCE));
    }

    @Test
    public void rendersConsolePlusSourceTabsWithCorrectLabels() {
        final List<NeutralTab> tabs = Arrays.asList(
                NeutralTab.console("groovy", "gremlin> g.V(1)"),
                NeutralTab.source("groovy", "g.V(1)"));
        final String html = renderer.render(null, tabs);
        assertThat(html, containsString("<section class=\"tabs tabs-2\">"));
        assertThat(html, containsString("class=\"tab-label-1\">console (groovy)</label>"));
        assertThat(html, containsString("class=\"tab-label-2\">groovy</label>"));
    }

    @Test
    public void rendersMultiLanguageTabGroup() {
        final List<NeutralTab> tabs = Arrays.asList(
                NeutralTab.console("groovy", "gremlin> g.V(1)"),
                NeutralTab.source("groovy", "g.V(1)"),
                NeutralTab.source("java", "g.V(1)"),
                NeutralTab.source("python", "g.V(1)"));
        final String html = renderer.render(null, tabs);
        assertThat(html, containsString("<section class=\"tabs tabs-4\">"));
        assertThat(html, containsString("class=\"tab-label-3\">java</label>"));
        assertThat(html, containsString("class=\"tab-label-4\">python</label>"));
    }

    @Test
    public void escapesHtmlInSourceContentViaFallbackPath() {
        final List<NeutralTab> tabs = Collections.singletonList(
                NeutralTab.source("java", "List<String> x = new ArrayList<>()"));
        final String html = renderer.render(null, tabs);
        assertThat(html, containsString("List&lt;String&gt; x = new ArrayList&lt;&gt;()"));
    }

    @Test
    public void producesCodeRayPreBlocks() {
        final List<NeutralTab> tabs = Collections.singletonList(
                NeutralTab.source("java", "g.V()"));
        final String html = renderer.render(null, tabs);
        assertThat(html, containsString("<pre class=\"CodeRay highlight\"><code data-lang=\"java\">"));
    }
}
