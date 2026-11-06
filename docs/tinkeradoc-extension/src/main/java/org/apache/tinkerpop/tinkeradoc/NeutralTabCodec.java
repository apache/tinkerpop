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

import java.util.ArrayList;
import java.util.List;

/**
 * Serializes and deserializes a list of {@link NeutralTab}s as a compact JSON array. This is the
 * on-the-wire form of the neutral tab model that the execution pass writes into a neutral custom
 * block and that each backend converter reads back.
 * <p>
 * The schema is fixed and internal: an array of objects
 * <code>{"label":…,"language":…,"kind":"CONSOLE"|"SOURCE","content":…}</code>. Because the schema
 * is known and small, this codec is intentionally self-contained (no external JSON dependency).
 * It is a strict round-trip: {@code parse(serialize(tabs))} reproduces the input exactly, including
 * arbitrary content with quotes, backslashes, and control characters.
 */
final class NeutralTabCodec {

    private NeutralTabCodec() {
    }

    /**
     * Serializes tabs to a JSON array string.
     */
    static String serialize(final List<NeutralTab> tabs) {
        final StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < tabs.size(); i++) {
            if (i > 0) sb.append(',');
            final NeutralTab tab = tabs.get(i);
            sb.append('{');
            appendMember(sb, "label", tab.getLabel(), true);
            appendMember(sb, "language", tab.getLanguage(), false);
            appendMember(sb, "kind", tab.getKind().name(), false);
            appendMember(sb, "content", tab.getContent(), false);
            sb.append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * Parses a JSON array string (as produced by {@link #serialize}) back into tabs.
     *
     * @throws IllegalArgumentException if the input is not well-formed for this schema
     */
    static List<NeutralTab> parse(final String json) {
        final Parser parser = new Parser(json);
        final List<NeutralTab> tabs = new ArrayList<>();
        parser.skipWs();
        parser.expect('[');
        parser.skipWs();
        if (parser.peek() == ']') {
            parser.next();
            return tabs;
        }
        while (true) {
            parser.skipWs();
            tabs.add(parseTab(parser));
            parser.skipWs();
            final char c = parser.next();
            if (c == ']') break;
            if (c != ',') throw parser.error("expected ',' or ']'");
        }
        return tabs;
    }

    private static NeutralTab parseTab(final Parser parser) {
        parser.expect('{');
        String label = null;
        String language = null;
        String kind = null;
        String content = null;
        parser.skipWs();
        if (parser.peek() == '}') {
            parser.next();
            throw parser.error("empty tab object");
        }
        while (true) {
            parser.skipWs();
            final String key = parser.parseString();
            parser.skipWs();
            parser.expect(':');
            parser.skipWs();
            final String value = parser.parseString();
            switch (key) {
                case "label": label = value; break;
                case "language": language = value; break;
                case "kind": kind = value; break;
                case "content": content = value; break;
                default: throw parser.error("unexpected key: " + key);
            }
            parser.skipWs();
            final char c = parser.next();
            if (c == '}') break;
            if (c != ',') throw parser.error("expected ',' or '}'");
        }
        if (label == null || language == null || kind == null || content == null) {
            throw parser.error("missing member in tab object");
        }
        return new NeutralTab(label, language, NeutralTab.Kind.valueOf(kind), content);
    }

    private static void appendMember(final StringBuilder sb, final String key, final String value,
                                     final boolean first) {
        if (!first) sb.append(',');
        writeString(sb, key);
        sb.append(':');
        writeString(sb, value);
    }

    private static void writeString(final StringBuilder sb, final String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }

    /** A minimal recursive-descent scanner for the fixed neutral-tab JSON schema. */
    private static final class Parser {
        private final String s;
        private int pos;

        Parser(final String s) {
            this.s = s;
        }

        void skipWs() {
            while (pos < s.length()) {
                final char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++;
                else break;
            }
        }

        char peek() {
            if (pos >= s.length()) throw error("unexpected end of input");
            return s.charAt(pos);
        }

        char next() {
            if (pos >= s.length()) throw error("unexpected end of input");
            return s.charAt(pos++);
        }

        void expect(final char expected) {
            final char c = next();
            if (c != expected) throw error("expected '" + expected + "' but found '" + c + "'");
        }

        String parseString() {
            expect('"');
            final StringBuilder sb = new StringBuilder();
            while (true) {
                final char c = next();
                if (c == '"') break;
                if (c == '\\') {
                    final char esc = next();
                    switch (esc) {
                        case '"': sb.append('"'); break;
                        case '\\': sb.append('\\'); break;
                        case '/': sb.append('/'); break;
                        case 'n': sb.append('\n'); break;
                        case 'r': sb.append('\r'); break;
                        case 't': sb.append('\t'); break;
                        case 'b': sb.append('\b'); break;
                        case 'f': sb.append('\f'); break;
                        case 'u':
                            if (pos + 4 > s.length()) throw error("truncated \\u escape");
                            final String hex = s.substring(pos, pos + 4);
                            pos += 4;
                            sb.append((char) Integer.parseInt(hex, 16));
                            break;
                        default: throw error("invalid escape: \\" + esc);
                    }
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        IllegalArgumentException error(final String message) {
            return new IllegalArgumentException("Invalid neutral-tab JSON at position " + pos + ": " + message);
        }
    }
}
