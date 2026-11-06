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

import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.Postprocessor;

import java.util.regex.Pattern;

/**
 * Postprocessor that finalizes rendered output for both backends. It replaces {@code x.y.z} version
 * placeholders with the actual TinkerPop version (applies to HTML and Markdown alike) and, for the
 * HTML backend only, removes the empty comment spans CodeRay emits.
 */
public class GremlinPostprocessor extends Postprocessor {

    // Matches empty comment spans from CodeRay: <span class="comment">/* */</span>. HTML-only.
    private static final Pattern EMPTY_COMMENT_SPAN_PATTERN = Pattern.compile(
            "<span class=\"comment\">/\\*\\s*\\*/</span>");

    @Override
    public String process(final Document document, final String output) {
        String result = output;

        // 1. Remove empty comment spans from CodeRay — an HTML-highlighting artifact that has no
        //    place in Markdown output, so only strip it for the HTML backend.
        if (isHtmlBackend(document)) {
            result = EMPTY_COMMENT_SPAN_PATTERN.matcher(result).replaceAll("");
        }

        // 2. Replace x.y.z with the actual version. This applies to every backend so both the HTML
        //    and the Markdown mirror carry the concrete version string.
        final String version = resolveVersion(document);
        if (version != null) {
            result = result.replace("x.y.z", version);
        }

        return result;
    }

    /**
     * Whether the active backend is an HTML backend. The Markdown backend ({@code tpmarkdown}) must
     * skip HTML-specific cleanups.
     */
    private static boolean isHtmlBackend(final Document document) {
        final Object backend = document.getAttribute("backend");
        // Default to HTML when unknown so existing HTML behavior is never weakened.
        if (backend == null) return true;
        final String b = backend.toString();
        return b.startsWith("html") || b.equals("xhtml") || b.equals("html5");
    }

    private String resolveVersion(final Document document) {
        Object version = document.getAttribute("tinkerpop-version");
        if (version != null) {
            return version.toString();
        }
        version = document.getAttribute("revnumber");
        if (version != null) {
            return version.toString();
        }
        return null;
    }
}
