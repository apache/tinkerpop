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

import org.apache.tinkerpop.gremlin.language.translator.GremlinTranslator;
import org.apache.tinkerpop.gremlin.language.translator.Translation;
import org.apache.tinkerpop.gremlin.language.translator.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates canonical Gremlin into all supported language variants using {@link GremlinTranslator}.
 */
public class VariantTranslator {

    private static final Logger log = LoggerFactory.getLogger(VariantTranslator.class);

    /**
     * The language variants to generate, in display order. Excludes CANONICAL, ANONYMIZED, GROOVY
     * (which is essentially the same as the console output), and LANGUAGE (deprecated).
     */
    static final List<Translator> VARIANT_LANGUAGES = Collections.unmodifiableList(Arrays.asList(
            Translator.JAVA,
            Translator.PYTHON,
            Translator.JAVASCRIPT,
            Translator.DOTNET,
            Translator.GO
    ));

    /**
     * Display names for tab labels.
     */
    private static final Map<Translator, String> DISPLAY_NAMES = new LinkedHashMap<>();
    static {
        DISPLAY_NAMES.put(Translator.JAVA, "java");
        DISPLAY_NAMES.put(Translator.PYTHON, "python");
        DISPLAY_NAMES.put(Translator.JAVASCRIPT, "javascript");
        DISPLAY_NAMES.put(Translator.DOTNET, "c#");
        DISPLAY_NAMES.put(Translator.GO, "go");
    }

    /**
     * Asciidoc source language identifiers for syntax highlighting.
     */
    private static final Map<Translator, String> SOURCE_LANGUAGES = new LinkedHashMap<>();
    static {
        SOURCE_LANGUAGES.put(Translator.JAVA, "java");
        SOURCE_LANGUAGES.put(Translator.PYTHON, "python");
        SOURCE_LANGUAGES.put(Translator.JAVASCRIPT, "javascript");
        SOURCE_LANGUAGES.put(Translator.DOTNET, "csharp");
        SOURCE_LANGUAGES.put(Translator.GO, "go");
    }

    public static String getDisplayName(final Translator translator) {
        return DISPLAY_NAMES.getOrDefault(translator, translator.getName().toLowerCase());
    }

    public static String getSourceLanguage(final Translator translator) {
        return SOURCE_LANGUAGES.getOrDefault(translator, translator.getName().toLowerCase());
    }

    /**
     * Translates a single Gremlin statement to all variant languages. Returns a map from
     * {@link Translator} to the translated code string. Statements that fail to parse
     * (e.g. those containing lambdas or non-standard Groovy) are skipped with a warning.
     */
    public static Map<Translator, String> translateStatement(final String gremlin) {
        final Map<Translator, String> results = new LinkedHashMap<>();
        for (final Translator lang : VARIANT_LANGUAGES) {
            try {
                final Translation t = GremlinTranslator.translate(gremlin, "g", lang);
                results.put(lang, t.getTranslated());
            } catch (final Exception e) {
                log.debug("Cannot translate to {}: {} — {}", lang.getName(), gremlin, e.getMessage());
            }
        }
        return results;
    }

    /**
     * Translates multiple Gremlin statements and joins them with newlines per language.
     * If any statement fails to translate for a given language, that language is omitted entirely.
     */
    public static Map<Translator, String> translateBlock(final List<String> statements) {
        final Map<Translator, String> results = new LinkedHashMap<>();

        for (final Translator lang : VARIANT_LANGUAGES) {
            final StringBuilder sb = new StringBuilder();
            boolean allTranslated = true;

            for (final String stmt : statements) {
                try {
                    final Translation t = GremlinTranslator.translate(stmt, "g", lang);
                    if (sb.length() > 0) sb.append("\n");
                    sb.append(t.getTranslated());
                } catch (final Exception e) {
                    log.debug("Cannot translate to {}: {} — {}", lang.getName(), stmt, e.getMessage());
                    allTranslated = false;
                    break;
                }
            }

            if (allTranslated) {
                results.put(lang, sb.toString());
            }
        }

        return results;
    }
}
