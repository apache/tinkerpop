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
package org.apache.tinkerpop.gremlin.docs;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link TabbedHtmlBuilder}.
 */
public class TabbedHtmlBuilderTest {

    private TabbedHtmlBuilder builder;

    @Before
    public void setUp() {
        builder = new TabbedHtmlBuilder();
    }

    @Test
    public void shouldReturnEmptyStringForNullTabs() {
        assertThat(builder.build(null), is(""));
    }

    @Test
    public void shouldReturnEmptyStringForEmptyTabs() {
        assertThat(builder.build(Collections.emptyList()), is(""));
    }

    @Test
    public void shouldGenerateSectionWithTabsClass() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.consoleTab("groovy", "gremlin> g.V()"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("<section class=\"tabs tabs-1\">"));
        assertThat(html, containsString("</section>"));
    }

    @Test
    public void shouldGenerateCorrectTabCountClass() {
        final List<TabbedHtmlBuilder.Tab> tabs = Arrays.asList(
                TabbedHtmlBuilder.consoleTab("groovy", "output"),
                TabbedHtmlBuilder.codeTab("java", "g.V()"),
                TabbedHtmlBuilder.codeTab("python", "g.V()"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("<section class=\"tabs tabs-3\">"));
    }

    @Test
    public void shouldGenerateRadioInputsWithCorrectAttributes() {
        final List<TabbedHtmlBuilder.Tab> tabs = Arrays.asList(
                TabbedHtmlBuilder.consoleTab("groovy", "output"),
                TabbedHtmlBuilder.codeTab("java", "code"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("type=\"radio\""));
        assertThat(html, containsString("name=\"tab-group-1\""));
        assertThat(html, containsString("id=\"tab-1-1\""));
        assertThat(html, containsString("id=\"tab-1-2\""));
        assertThat(html, containsString("class=\"tab-selector-1\""));
        assertThat(html, containsString("class=\"tab-selector-2\""));
    }

    @Test
    public void shouldCheckFirstTabByDefault() {
        final List<TabbedHtmlBuilder.Tab> tabs = Arrays.asList(
                TabbedHtmlBuilder.consoleTab("groovy", "output"),
                TabbedHtmlBuilder.codeTab("java", "code"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("id=\"tab-1-1\" class=\"tab-selector-1\" checked"));
        assertThat(html, not(containsString("id=\"tab-1-2\" class=\"tab-selector-2\" checked")));
    }

    @Test
    public void shouldGenerateLabelsWithCorrectForAttribute() {
        final List<TabbedHtmlBuilder.Tab> tabs = Arrays.asList(
                TabbedHtmlBuilder.consoleTab("groovy", "output"),
                TabbedHtmlBuilder.codeTab("java", "code"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("<label for=\"tab-1-1\" class=\"tab-label-1\">console (groovy)</label>"));
        assertThat(html, containsString("<label for=\"tab-1-2\" class=\"tab-label-2\">java</label>"));
    }

    @Test
    public void shouldNotGenerateClearShadowDiv() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.consoleTab("groovy", "output"));
        final String html = builder.build(tabs);
        assertThat(html, not(containsString("<div class=\"clear-shadow\"></div>")));
    }

    @Test
    public void shouldGenerateTabContentContainer() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.consoleTab("groovy", "output"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("<div class=\"tabcontent\">"));
        assertThat(html, containsString("<div class=\"tabcontent-1\">"));
    }

    @Test
    public void shouldGenerateCodeRayPreBlocks() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.codeTab("java", "g.V()"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("<pre class=\"CodeRay highlight\"><code data-lang=\"java\">"));
        assertThat(html, containsString("</code></pre>"));
    }

    @Test
    public void shouldEscapeHtmlInContent() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.codeTab("java", "List<String> x = new ArrayList<>()"));
        final String html = builder.build(tabs);
        assertThat(html, containsString("List&lt;String&gt; x = new ArrayList&lt;&gt;()"));
    }

    @Test
    public void shouldEscapeHtmlInLabels() {
        final TabbedHtmlBuilder.Tab tab = new TabbedHtmlBuilder.Tab("<script>", "java", "code");
        final String html = builder.build(Collections.singletonList(tab));
        assertThat(html, containsString("&lt;script&gt;</label>"));
    }

    @Test
    public void shouldIncrementGroupCounterSequentially() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.codeTab("java", "code"));

        final String html1 = builder.build(tabs);
        final String html2 = builder.build(tabs);

        assertThat(html1, containsString("name=\"tab-group-1\""));
        assertThat(html2, containsString("name=\"tab-group-2\""));
        assertThat(builder.getGroupCounter(), is(2));
    }

    @Test
    public void shouldResetCounter() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.codeTab("java", "code"));
        builder.build(tabs);
        assertThat(builder.getGroupCounter(), is(1));

        builder.resetCounter();
        assertThat(builder.getGroupCounter(), is(0));

        final String html = builder.build(tabs);
        assertThat(html, containsString("name=\"tab-group-1\""));
    }

    @Test
    public void shouldCreateConsoleTabWithCorrectLabel() {
        final TabbedHtmlBuilder.Tab tab = TabbedHtmlBuilder.consoleTab("groovy", "gremlin> g.V()");
        assertThat(tab.getLabel(), is("console (groovy)"));
        assertThat(tab.getLanguage(), is("groovy"));
        assertThat(tab.getContent(), is("gremlin> g.V()"));
    }

    @Test
    public void shouldCreateCodeTabWithCorrectLabel() {
        final TabbedHtmlBuilder.Tab tab = TabbedHtmlBuilder.codeTab("python", "g.V()");
        assertThat(tab.getLabel(), is("python"));
        assertThat(tab.getLanguage(), is("python"));
    }

    @Test
    public void shouldTransformTrailingCalloutMarkers() {
        final String source = "g.V() <1>\ng.E() <2>";
        final String result = TabbedHtmlBuilder.transformCallouts(source);
        assertThat(result, is("g.V() // <1>\ng.E() // <2>"));
    }

    @Test
    public void shouldTransformMultipleCalloutsOnSameLine() {
        final String source = "g.V() <1> <2>";
        final String result = TabbedHtmlBuilder.transformCallouts(source);
        assertThat(result, is("g.V() // <1> // <2>"));
    }

    @Test
    public void shouldNotTransformCalloutsInMiddleOfLine() {
        final String source = "x = list.get(<1>)";
        final String result = TabbedHtmlBuilder.transformCallouts(source);
        assertThat(result, is("x = list.get(<1>)"));
    }

    @Test
    public void shouldHandleNullSourceInTransformCallouts() {
        assertThat(TabbedHtmlBuilder.transformCallouts(null), is((String) null));
    }

    @Test
    public void shouldHandleEmptySourceInTransformCallouts() {
        assertThat(TabbedHtmlBuilder.transformCallouts(""), is(""));
    }

    @Test
    public void shouldPreserveLineWithoutCallouts() {
        final String source = "g.V().hasLabel('person')";
        final String result = TabbedHtmlBuilder.transformCallouts(source);
        assertThat(result, is(source));
    }

    @Test
    public void shouldApplyCalloutTransformInCodeTab() {
        final TabbedHtmlBuilder.Tab tab = TabbedHtmlBuilder.codeTab("java", "g.V() <1>");
        assertThat(tab.getContent(), is("g.V() // <1>"));
    }

    @Test
    public void shouldNotApplyCalloutTransformInConsoleTab() {
        final TabbedHtmlBuilder.Tab tab = TabbedHtmlBuilder.consoleTab("groovy", "gremlin> g.V() <1>");
        assertThat(tab.getContent(), is("gremlin> g.V() <1>"));
    }

    @Test
    public void shouldGenerateFullStructureForMultipleTabs() {
        final List<TabbedHtmlBuilder.Tab> tabs = Arrays.asList(
                TabbedHtmlBuilder.consoleTab("groovy", "gremlin> g.V()"),
                TabbedHtmlBuilder.codeTab("java", "g.V()"),
                TabbedHtmlBuilder.codeTab("python", "g.V()"),
                TabbedHtmlBuilder.codeTab("csharp", "g.V()"));
        final String html = builder.build(tabs);

        // Verify overall structure
        assertThat(html, containsString("<section class=\"tabs tabs-4\">"));
        assertThat(html, containsString("name=\"tab-group-1\""));

        // Verify all 4 inputs
        assertThat(html, containsString("id=\"tab-1-1\" class=\"tab-selector-1\" checked"));
        assertThat(html, containsString("id=\"tab-1-2\" class=\"tab-selector-2\""));
        assertThat(html, containsString("id=\"tab-1-3\" class=\"tab-selector-3\""));
        assertThat(html, containsString("id=\"tab-1-4\" class=\"tab-selector-4\""));

        // Verify all 4 labels
        assertThat(html, containsString("class=\"tab-label-1\">console (groovy)</label>"));
        assertThat(html, containsString("class=\"tab-label-2\">java</label>"));
        assertThat(html, containsString("class=\"tab-label-3\">python</label>"));
        assertThat(html, containsString("class=\"tab-label-4\">csharp</label>"));

        // Verify all 4 content divs
        assertThat(html, containsString("<div class=\"tabcontent-1\">"));
        assertThat(html, containsString("<div class=\"tabcontent-2\">"));
        assertThat(html, containsString("<div class=\"tabcontent-3\">"));
        assertThat(html, containsString("<div class=\"tabcontent-4\">"));
    }

    @Test
    public void shouldProduceDeterministicIdsAcrossMultipleGroups() {
        final List<TabbedHtmlBuilder.Tab> tabs = Collections.singletonList(
                TabbedHtmlBuilder.codeTab("java", "code"));

        final String html1 = builder.build(tabs);
        final String html2 = builder.build(tabs);
        final String html3 = builder.build(tabs);

        assertThat(html1, containsString("id=\"tab-1-1\""));
        assertThat(html2, containsString("id=\"tab-2-1\""));
        assertThat(html3, containsString("id=\"tab-3-1\""));
    }
}
