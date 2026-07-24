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

/**
 * A format-neutral representation of one tab in a Gremlin example: a label, a language, a kind,
 * and the tab's content as <em>plain text</em> (no highlighting, no HTML). Callout markers are
 * retained inline in the content as AsciiDoc {@code <n>} markers; each backend renderer decides
 * how to present them (HTML conums, Markdown comments, etc.).
 * <p>
 * This is the seam that lets a single execution of the live Gremlin console feed multiple output
 * backends: the executor produces {@code NeutralTab}s, and {@link HtmlTabRenderer} (and, later, a
 * Markdown renderer) turn them into their respective formats.
 */
class NeutralTab {

    /** Whether a tab holds an interactive console transcript or a clean code sample. */
    enum Kind {
        /** A {@code gremlin>}-prefixed transcript including executed results. */
        CONSOLE,
        /** A clean code sample with no prompts or results. */
        SOURCE
    }

    private final String label;
    private final String language;
    private final Kind kind;
    private final String content;

    NeutralTab(final String label, final String language, final Kind kind, final String content) {
        this.label = label;
        this.language = language;
        this.kind = kind;
        this.content = content;
    }

    /**
     * Creates a console-transcript tab. Its label matches the existing HTML output, i.e.
     * {@code console (<lang>)}.
     */
    static NeutralTab console(final String language, final String content) {
        return new NeutralTab("console (" + language + ")", language, Kind.CONSOLE, content);
    }

    /**
     * Creates a clean code-sample tab. Its label is the language identifier, matching the existing
     * HTML output.
     */
    static NeutralTab source(final String language, final String content) {
        return new NeutralTab(language, language, Kind.SOURCE, content);
    }

    String getLabel() {
        return label;
    }

    String getLanguage() {
        return language;
    }

    Kind getKind() {
        return kind;
    }

    String getContent() {
        return content;
    }
}
