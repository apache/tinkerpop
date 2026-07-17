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
 * Postprocessor that applies callout fixes, removes empty comment spans,
 * and replaces x.y.z version placeholders with the actual TinkerPop version.
 */
public class GremlinPostprocessor extends Postprocessor {

    // Matches <i class="conum"> or <b class="conum"> (with possible other classes)
    private static final Pattern CONUM_PATTERN = Pattern.compile(
            "<([ib])\\s+class=\"conum\"");

    // Matches // preceding a callout marker, wraps in hide-when-copy span
    private static final Pattern COMMENT_BEFORE_CONUM_PATTERN = Pattern.compile(
            "//\\s*(<[ib] class=\"conum)");

    // Matches empty comment spans from CodeRay: <span class="comment">/* */</span>
    private static final Pattern EMPTY_COMMENT_SPAN_PATTERN = Pattern.compile(
            "<span class=\"comment\">/\\*\\s*\\*/</span>");

    @Override
    public String process(final Document document, final String output) {
        String result = output;

        // 1. Remove empty comment spans from CodeRay
        result = EMPTY_COMMENT_SPAN_PATTERN.matcher(result).replaceAll("");

        // 2. Replace x.y.z with actual version
        final String version = resolveVersion(document);
        if (version != null) {
            result = result.replace("x.y.z", version);
        }

        return result;
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
