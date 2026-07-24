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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Renders a list of format-neutral {@link NeutralTab}s into Markdown. This is the Markdown backend
 * for the neutral tab model, parallel to {@link HtmlTabRenderer}.
 * <p>
 * HTML uses a CSS radio-tab widget; Markdown has no tabs, so each tab is flattened to a labeled
 * fenced code block emitted in order. The console tab renders as a {@code text} fence (it is an
 * interactive transcript with {@code gremlin>} prompts and {@code ==>} results, not compilable
 * source); source tabs render with their language as the fence info string. AsciiDoc callout
 * markers ({@code <1>}) in a tab's content are converted to inline {@code // (n)} comments, since
 * Markdown fenced blocks have no callout facility.
 */
class MarkdownTabRenderer {

    private static final Pattern TRAILING_CALLOUTS = Pattern.compile("\\s*((<\\d+>\\s*)+)$");
    private static final Pattern CALLOUT_NUMBER = Pattern.compile("<(\\d+)>");

    /**
     * Renders the given neutral tabs as a sequence of labeled fenced code blocks.
     *
     * @param tabs the neutral tabs to render
     * @return the Markdown for the tab group (no trailing newline)
     */
    String render(final List<NeutralTab> tabs) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tabs.size(); i++) {
            if (i > 0) sb.append("\n\n");
            final NeutralTab tab = tabs.get(i);
            // A single Groovy console+source pair (the common case) needs no per-tab heading; extra
            // language variants are labeled so the reader can tell them apart without tab chrome.
            sb.append("**").append(tab.getLabel()).append("**\n\n");
            final String fenceLang = tab.getKind() == NeutralTab.Kind.CONSOLE ? "text" : fenceLang(tab.getLanguage());
            sb.append("```").append(fenceLang).append("\n");
            sb.append(convertCallouts(tab.getContent()));
            sb.append("\n```");
        }
        return sb.toString();
    }

    /**
     * Maps a tab language to a Markdown fence info string. Unknown/blank languages produce a bare
     * fence.
     */
    private static String fenceLang(final String language) {
        return language == null ? "" : language.trim();
    }

    /**
     * Converts trailing AsciiDoc callout markers on each line ({@code <1>}) into inline
     * {@code // (n)} comments. Non-trailing angle brackets are left untouched.
     */
    static String convertCallouts(final String content) {
        if (content == null || content.isEmpty()) return "";
        final String[] lines = content.split("\n", -1);
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) result.append("\n");
            result.append(convertCalloutLine(lines[i]));
        }
        return result.toString();
    }

    private static String convertCalloutLine(final String line) {
        final Matcher m = TRAILING_CALLOUTS.matcher(line);
        if (!m.find()) return line;
        final String prefix = stripTrailing(line.substring(0, m.start()));
        final StringBuilder sb = new StringBuilder(prefix);
        final Matcher nums = CALLOUT_NUMBER.matcher(m.group(1));
        while (nums.find()) {
            sb.append(" // (").append(nums.group(1)).append(')');
        }
        return sb.toString();
    }

    private static String stripTrailing(final String s) {
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) end--;
        return s.substring(0, end);
    }
}
