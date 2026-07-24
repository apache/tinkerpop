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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link MarkdownTabRenderer}: the Markdown backend for the neutral tab model.
 */
public class MarkdownTabRendererTest {

    private MarkdownTabRenderer renderer;

    @Before
    public void setUp() {
        renderer = new MarkdownTabRenderer();
    }

    @Test
    public void consoleTabRendersAsTextFence() {
        final String md = renderer.render(Collections.singletonList(
                NeutralTab.console("groovy", "gremlin> g.V()\n==>v[1]")));
        assertThat(md, containsString("**console (groovy)**"));
        assertThat(md, containsString("```text\ngremlin> g.V()\n==>v[1]\n```"));
    }

    @Test
    public void sourceTabRendersWithLanguageFence() {
        final String md = renderer.render(Collections.singletonList(
                NeutralTab.source("java", "g.V(1)")));
        assertThat(md, containsString("**java**"));
        assertThat(md, containsString("```java\ng.V(1)\n```"));
    }

    @Test
    public void multipleTabsEmittedInOrderSeparatedByBlankLines() {
        final List<NeutralTab> tabs = Arrays.asList(
                NeutralTab.console("groovy", "gremlin> g.V(1)"),
                NeutralTab.source("groovy", "g.V(1)"),
                NeutralTab.source("python", "g.V(1)"));
        final String md = renderer.render(tabs);
        final int console = md.indexOf("**console (groovy)**");
        final int groovy = md.indexOf("**groovy**");
        final int python = md.indexOf("**python**");
        assertThat(console >= 0 && groovy > console && python > groovy, is(true));
        // Console must be a text fence; python a python fence.
        assertThat(md, containsString("```text\n"));
        assertThat(md, containsString("```python\n"));
    }

    @Test
    public void trailingCalloutBecomesInlineComment() {
        final String md = renderer.render(Collections.singletonList(
                NeutralTab.source("java", "g.V(1).out() <1>")));
        assertThat(md, containsString("g.V(1).out() // (1)"));
        assertThat(md, not(containsString("<1>")));
    }

    @Test
    public void multipleTrailingCalloutsBecomeMultipleComments() {
        final String md = renderer.render(Collections.singletonList(
                NeutralTab.source("java", "g.V() <1> <2>")));
        assertThat(md, containsString("g.V() // (1) // (2)"));
    }

    @Test
    public void midLineAngleBracketsAreNotTreatedAsCallouts() {
        final String md = renderer.render(Collections.singletonList(
                NeutralTab.source("java", "List<String> x")));
        assertThat(md, containsString("List<String> x"));
        assertThat(md, not(containsString("// (")));
    }

    @Test
    public void emptyContentProducesEmptyFenceBody() {
        final String md = renderer.render(Collections.singletonList(
                NeutralTab.console("groovy", "")));
        assertThat(md, containsString("```text\n\n```"));
    }

    @Test
    public void calloutConversionPerLine() {
        final String md = renderer.render(Collections.singletonList(
                NeutralTab.source("groovy", "a() <1>\nb()\nc() <2>")));
        assertThat(md, containsString("a() // (1)\nb()\nc() // (2)"));
    }
}
