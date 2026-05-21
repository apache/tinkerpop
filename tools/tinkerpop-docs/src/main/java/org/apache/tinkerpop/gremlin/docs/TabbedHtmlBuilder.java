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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generates CSS-only tabbed HTML matching the existing {@code tinkerpop.css} tab styles.
 * <p>
 * The generated structure uses radio inputs for tab switching without JavaScript:
 * <pre>
 * &lt;section class="tabs tabs-N"&gt;
 *   &lt;input type="radio" name="tab-group-ID" id="tab-ID-1" class="tab-selector-1" checked&gt;
 *   &lt;label for="tab-ID-1" class="tab-label-1"&gt;Label&lt;/label&gt;
 *   ...
 *   &lt;div class="clear-shadow"&gt;&lt;/div&gt;
 *   &lt;div class="tabcontent"&gt;
 *     &lt;div class="tabcontent-1"&gt;...&lt;/div&gt;
 *     ...
 *   &lt;/div&gt;
 * &lt;/section&gt;
 * </pre>
 */
public class TabbedHtmlBuilder {

    private static final Pattern CALLOUT_PATTERN = Pattern.compile("<(\\d+)>");

    private int groupCounter = 0;

    /**
     * A single tab entry with a label, language, and source code content.
     */
    static class Tab {
        private final String label;
        private final String language;
        private final String content;

        Tab(final String label, final String language, final String content) {
            this.label = label;
            this.language = language;
            this.content = content;
        }

        String getLabel() {
            return label;
        }

        String getLanguage() {
            return language;
        }

        String getContent() {
            return content;
        }
    }

    /**
     * Returns the current group counter value (for testing).
     */
    int getGroupCounter() {
        return groupCounter;
    }

    /**
     * Resets the group counter (for testing or reprocessing).
     */
    void resetCounter() {
        groupCounter = 0;
    }

    /**
     * Builds tabbed HTML for a list of tabs.
     *
     * @param tabs the tabs to render
     * @return the complete HTML string for the tab group
     */
    String build(final List<Tab> tabs) {
        if (tabs == null || tabs.isEmpty()) {
            return "";
        }

        final int groupId = ++groupCounter;
        final int tabCount = tabs.size();
        final StringBuilder html = new StringBuilder();

        html.append("<section class=\"tabs tabs-").append(tabCount).append("\">\n");

        // Radio inputs and labels
        for (int i = 0; i < tabCount; i++) {
            final int tabNum = i + 1;
            final String inputId = "tab-" + groupId + "-" + tabNum;
            final String checked = (i == 0) ? " checked" : "";

            html.append("  <input type=\"radio\" name=\"tab-group-")
                    .append(groupId).append("\" id=\"").append(inputId)
                    .append("\" class=\"tab-selector-").append(tabNum).append("\"")
                    .append(checked).append(">\n");
            html.append("  <label for=\"").append(inputId)
                    .append("\" class=\"tab-label-").append(tabNum).append("\">")
                    .append(escapeHtml(tabs.get(i).getLabel())).append("</label>\n");
        }

        html.append("  <div class=\"clear-shadow\"></div>\n");
        html.append("  <div class=\"tabcontent\">\n");

        // Tab content panels
        for (int i = 0; i < tabCount; i++) {
            final Tab tab = tabs.get(i);
            final int tabNum = i + 1;
            html.append("    <div class=\"tabcontent-").append(tabNum).append("\">\n");
            html.append("<div class=\"listingblock\">\n<div class=\"content\">\n");
            html.append("<pre class=\"CodeRay highlight\"><code data-lang=\"")
                    .append(escapeHtml(tab.getLanguage())).append("\">")
                    .append(renderContent(tab.getContent()))
                    .append("</code></pre>\n");
            html.append("</div>\n</div>\n");
            html.append("    </div>\n");
        }

        html.append("  </div>\n");
        html.append("</section>");

        return html.toString();
    }

    /**
     * Creates a console tab for a gremlin-groovy block.
     *
     * @param lang          the language (e.g., "groovy")
     * @param consoleOutput the formatted console output
     * @return a Tab instance for the console output
     */
    static Tab consoleTab(final String lang, final String consoleOutput) {
        return new Tab("console (" + lang + ")", lang, consoleOutput);
    }

    /**
     * Creates a code tab for a manual language variant block.
     *
     * @param lang   the language identifier
     * @param source the source code
     * @return a Tab instance for the code
     */
    static Tab codeTab(final String lang, final String source) {
        return new Tab(lang, lang, transformCallouts(source));
    }

    /**
     * Transforms AsciiDoc callout markers (e.g., {@code <1>}) into comment form ({@code // <1>}).
     *
     * @param source the source code potentially containing callout markers
     * @return the source with callout markers transformed to comments
     */
    static String transformCallouts(final String source) {
        if (source == null || source.isEmpty()) {
            return source;
        }

        final StringBuilder result = new StringBuilder();
        final String[] lines = source.split("\\r?\\n");
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                result.append("\n");
            }
            result.append(transformCalloutLine(lines[i]));
        }
        return result.toString();
    }

    /**
     * Transforms callout markers at the end of a line into comment form.
     */
    private static String transformCalloutLine(final String line) {
        final Matcher matcher = CALLOUT_PATTERN.matcher(line);
        final StringBuilder sb = new StringBuilder();
        int lastEnd = 0;
        final List<String> callouts = new ArrayList<>();

        while (matcher.find()) {
            // Only transform callouts that appear at the end of the line (possibly with whitespace)
            final String afterMatch = line.substring(matcher.end()).trim();
            if (afterMatch.isEmpty() || CALLOUT_PATTERN.matcher(afterMatch).matches()) {
                callouts.add(matcher.group(1));
                if (sb.length() == 0) {
                    sb.append(line, 0, matcher.start());
                }
            } else {
                // Not a trailing callout, keep as-is
                return line;
            }
            lastEnd = matcher.end();
        }

        if (callouts.isEmpty()) {
            return line;
        }

        // Trim trailing whitespace before callouts and append as comments
        final String codePart = sb.toString().stripTrailing();
        final StringBuilder result = new StringBuilder(codePart);
        for (final String num : callouts) {
            result.append(" // <").append(num).append(">");
        }
        return result.toString();
    }

    private static String escapeHtml(final String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    /**
     * Escapes HTML and then renders callout markers as proper conum elements.
     * Converts escaped {@code &lt;N&gt;} patterns into HTML callout spans.
     */
    private static String renderContent(final String text) {
        final String escaped = escapeHtml(text);
        // Process line by line to handle callouts at end of lines
        final String[] lines = escaped.split("\\n", -1);
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) result.append("\n");
            result.append(renderCallouts(lines[i]));
        }
        return result.toString();
    }

    /**
     * Replaces trailing callout markers on a line with HTML conum elements.
     */
    private static String renderCallouts(final String line) {
        // Match trailing callout markers: &lt;1&gt; or &lt;1&gt; &lt;2&gt; etc.
        final java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\\s*((&lt;\\d+&gt;\\s*)+)$").matcher(line);
        if (!m.find()) return line;
        final String prefix = line.substring(0, m.start());
        final String calloutsPart = m.group(1);
        final StringBuilder sb = new StringBuilder(prefix);
        final java.util.regex.Matcher nums = java.util.regex.Pattern
                .compile("&lt;(\\d+)&gt;").matcher(calloutsPart);
        while (nums.find()) {
            sb.append(" <span class=\"hide-when-copy\">//</span> <b class=\"conum invisible\">(")
              .append(nums.group(1)).append(")</b>");
        }
        return sb.toString();
    }
}
