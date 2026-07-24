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

import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns plain source text (a console transcript or a code sample) into CodeRay-highlighted HTML,
 * re-injecting AsciiDoc callout markers as conum spans.
 * <p>
 * This is the HTML-only rendering seam extracted from {@link GremlinTreeprocessor}: the executor
 * produces format-neutral plain text and callout markers, and this class is the one place that
 * knows how to render that neutrally-held content into HTML. It relies on the CodeRay highlighter
 * bundled with AsciidoctorJ, accessed through the JRuby runtime obtained from an AST node.
 */
class GremlinHighlighter {

    private static final Logger LOG = Logger.getLogger(GremlinHighlighter.class.getName());

    private static final Pattern TRAILING_CALLOUTS = Pattern.compile("\\s*((<\\d+>\\s*)+)$");
    private static final Pattern CALLOUT_NUMBER = Pattern.compile("<(\\d+)>");

    private org.jruby.runtime.builtin.IRubyObject coderayEncoder;
    private org.jruby.Ruby rubyRuntime;

    /**
     * Highlights {@code source} as {@code lang} using CodeRay via the JRuby runtime bundled with
     * AsciidoctorJ. Callout markers ({@code <1>}) at line ends are stripped before highlighting and
     * re-injected afterward as conum HTML, matching the output Asciidoctor produces for callouts.
     *
     * @param parent an AST node used to locate the JRuby runtime
     * @param lang   the source language passed to CodeRay
     * @param source the plain source text to highlight (may contain trailing callout markers)
     * @return highlighted HTML, or escaped plain text if highlighting is unavailable
     */
    String highlight(final StructuralNode parent, final String lang, final String source) {
        if (source == null || source.isEmpty()) return "";
        // Strip callouts before highlighting, re-inject after
        final String[] lines = source.split("\\r?\\n");
        final String[] calloutMarkers = new String[lines.length];
        final StringBuilder cleanSource = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            final Matcher m = TRAILING_CALLOUTS.matcher(lines[i]);
            if (m.find()) {
                calloutMarkers[i] = m.group(1);
                if (i > 0) cleanSource.append("\n");
                cleanSource.append(lines[i], 0, m.start());
            } else {
                calloutMarkers[i] = null;
                if (i > 0) cleanSource.append("\n");
                cleanSource.append(lines[i]);
            }
        }

        final String highlighted = doHighlight(parent, lang, cleanSource.toString());

        // Re-inject callouts as HTML conums
        final String[] highlightedLines = highlighted.split("\\n", -1);
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < highlightedLines.length; i++) {
            if (i > 0) result.append("\n");
            result.append(highlightedLines[i]);
            if (i < calloutMarkers.length && calloutMarkers[i] != null) {
                final Matcher nums = CALLOUT_NUMBER.matcher(calloutMarkers[i]);
                while (nums.find()) {
                    result.append(" <span class=\"comment\">//</span> <b class=\"conum\">(")
                          .append(nums.group(1)).append(")</b>");
                }
            }
        }
        return result.toString();
    }

    private String doHighlight(final StructuralNode parent, final String lang, final String source) {
        try {
            if (rubyRuntime == null) {
                rubyRuntime = org.asciidoctor.jruby.internal.JRubyRuntimeContext.get(parent);
                if (rubyRuntime == null) return escapeHtml(source);
                coderayEncoder = rubyRuntime.evalScriptlet(
                        "require 'coderay'; CodeRay::Duo[:groovy, :html, :css => :class]");
            }
            final org.jruby.RubyString rubySource = org.jruby.RubyString.newString(rubyRuntime, source);
            final org.jruby.runtime.builtin.IRubyObject result = coderayEncoder.callMethod(
                    rubyRuntime.getCurrentContext(), "highlight", rubySource);
            return result != null ? result.asJavaString() : escapeHtml(source);
        } catch (final Exception e) {
            LOG.warning("CodeRay highlighting failed, falling back to plain: " + e.getMessage());
            return escapeHtml(source);
        }
    }

    static String escapeHtml(final String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }
}
