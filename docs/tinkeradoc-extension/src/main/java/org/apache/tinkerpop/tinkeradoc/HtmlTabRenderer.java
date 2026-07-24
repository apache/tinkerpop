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

import org.asciidoctor.ast.StructuralNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Renders a list of format-neutral {@link NeutralTab}s into the CSS-only tabbed HTML widget.
 * <p>
 * This is the HTML backend for the neutral tab model: it highlights each tab's plain-text content
 * with {@link GremlinHighlighter} (CodeRay + conum injection) and hands the highlighted panels to
 * {@link TabbedHtmlBuilder}, which builds the radio-tab structure. The output is byte-for-byte the
 * same HTML the treeprocessor produced when it highlighted inline, so existing rendered docs are
 * unaffected. A parallel Markdown renderer will consume the same {@link NeutralTab}s later.
 */
class HtmlTabRenderer {

    private final GremlinHighlighter highlighter;
    private final TabbedHtmlBuilder tabBuilder;

    HtmlTabRenderer(final GremlinHighlighter highlighter, final TabbedHtmlBuilder tabBuilder) {
        this.highlighter = highlighter;
        this.tabBuilder = tabBuilder;
    }

    /**
     * Highlights and renders the given neutral tabs into a complete tab-group HTML string.
     *
     * @param parent the AST node used to locate the JRuby runtime for highlighting
     * @param tabs   the neutral tabs to render
     * @return the tab-group HTML
     */
    String render(final StructuralNode parent, final List<NeutralTab> tabs) {
        final List<TabbedHtmlBuilder.Tab> htmlTabs = new ArrayList<>(tabs.size());
        for (final NeutralTab tab : tabs) {
            // The existing pipeline highlighted every tab (console and source) as groovy-flavored
            // CodeRay regardless of the tab's declared language, so reproduce that exactly by
            // passing the tab language through to the highlighter (which uses groovy internally).
            final String highlighted = highlighter.highlight(parent, tab.getLanguage(), tab.getContent());
            if (tab.getKind() == NeutralTab.Kind.CONSOLE) {
                htmlTabs.add(TabbedHtmlBuilder.consoleTabHighlighted(tab.getLanguage(), highlighted));
            } else {
                htmlTabs.add(TabbedHtmlBuilder.codeTabHighlighted(tab.getLanguage(), highlighted));
            }
        }
        return tabBuilder.build(htmlTabs);
    }
}
