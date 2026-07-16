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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Processes Gremlin strings using regex to try to detect certain properties from the script without actual
 * having to execute a {@code eval()} on it.
 */
public class GremlinScriptChecker {

    /**
     * An empty result whose properties return as empty.
     */
    public static final Result EMPTY_RESULT = new Result(null, null, null, null, null);

    /**
     * At least one of these tokens should be present somewhere in the Gremlin string for {@link #parse(String)} to
     * take any action at all.
     */
    private static final Set<String> tokens = new HashSet<>(Arrays.asList("timeoutMillis", "TIMEOUT_MILLIS",
            "materializeProperties", "ARGS_MATERIALIZE_PROPERTIES",
            "language", "ARGS_LANGUAGE", "batchSize", "ARGS_BATCH_SIZE",
            "bulkResults", "BULK_RESULTS"));

    /**
     * Matches single line comments, multi-line comments and space characters.
     * <pre>
     * From https://regex101.com/
     *
     * 	OR: match either of the followings
     * Sequence: match all of the followings in order
     * / /
     * Repeat
     * AnyCharacterExcept\n
     * zero or more times
     * EndOfLine
     * Sequence: match all of the followings in order
     * /
     * *
     * Repeat
     * CapturingGroup
     * GroupNumber:1
     * OR: match either of the followings
     * AnyCharacterExcept\n
     * AnyCharIn[ CarriageReturn NewLine]
     * zero or more times (ungreedy)
     * *
     * /
     * WhiteSpaceCharacter
     * </pre>
     */
    private static final Pattern patternClean = Pattern.compile("//.*$|/\\*(.|[\\r\\n])*?\\*/|\\s", Pattern.MULTILINE);

    /**
     * Regex fragment for the timeout tokens to look for. There are basically two:
     * <ul>
     *     <li>{@code timeoutMillis} which is a string value and thus single or double quoted</li>
     *     <li>{@code TIMEOUT_MILLIS} which is a enum type of value which can be referenced with or without a {@code Tokens} qualifier</li>
     * </ul>
     * See {@link #patternWithOptions} for explain as this regex is embedded in there.
     */
    private static final String timeoutTokens = "[\"']timeoutMillis[\"']|(?:Tokens\\.)?TIMEOUT_MILLIS";

    /**
     * Regex fragment for the materializeProperties to look for. There are basically four:
     * <ul>
     *     <li>{@code materializeProperties} which is a string value and thus single or double quoted</li>
     *     <li>{@code ARGS_MATERIALIZE_PROPERTIES} which is a enum type of value which can be referenced with or without a {@code Tokens} qualifier</li>
     * </ul>
     */
    private static final String materializePropertiesTokens = "[\"']materializeProperties[\"']|(?:Tokens\\.)?ARGS_MATERIALIZE_PROPERTIES";

    /**
     * Regex fragment for the {@code language} tokens to look for:
     * <ul>
     *     <li>{@code language} which is a string value and thus single or double quoted</li>
     *     <li>{@code ARGS_LANGUAGE} which can be referenced with or without a {@code Tokens} qualifier</li>
     * </ul>
     */
    private static final String languageTokens = "[\"']language[\"']|(?:Tokens\\.)?ARGS_LANGUAGE";

    /**
     * Regex fragment for the {@code batchSize} tokens to look for:
     * <ul>
     *     <li>{@code batchSize} which is a string value and thus single or double quoted</li>
     *     <li>{@code ARGS_BATCH_SIZE} which can be referenced with or without a {@code Tokens} qualifier</li>
     * </ul>
     */
    private static final String batchSizeTokens = "[\"']batchSize[\"']|(?:Tokens\\.)?ARGS_BATCH_SIZE";

    /**
     * Regex fragment for the {@code bulkResults} tokens to look for:
     * <ul>
     *     <li>{@code bulkResults} which is a string value and thus single or double quoted</li>
     *     <li>{@code BULK_RESULTS} which can be referenced with or without a {@code Tokens} qualifier</li>
     * </ul>
     */
    private static final String bulkResultsTokens = "[\"']bulkResults[\"']|(?:Tokens\\.)?BULK_RESULTS";

    /**
     * Matches {@code .with({timeout-token},{timeout})} with a matching group on the {@code timeout}.
     * input: g.with('materializeProperties',100)
     * <pre>
     * From https://regex101.com/
     *
     * \.with\((((?:["']materializeProperties["']|["']scriptEvaluationTimeout["']|(?:Tokens\.)?ARGS_EVAL_TIMEOUT|(?:Tokens\.)?ARGS_SCRIPT_EVAL_TIMEOUT),(?<to>\d*)(:?L|l)?)|((?:["']materializeProperties["']|(?:Tokens\.)?ARGS_MATERIALIZE_PROPERTIES),["'](?<mp>.*?)["']?)|((?:["']requestId["']|(?:Tokens\.)?REQUEST_ID),["'](?<rid>.*?)["']))\)
     *
     * gm
     * \. matches the character . with index 4610 (2E16 or 568) literally (case sensitive)
     * with matches the characters with literally (case sensitive)
     * \( matches the character ( with index 4010 (2816 or 508) literally (case sensitive)
     * 1st Capturing Group (((?:["']materializeProperties["']|["']scriptEvaluationTimeout["']|(?:Tokens\.)?ARGS_EVAL_TIMEOUT|(?:Tokens\.)?ARGS_SCRIPT_EVAL_TIMEOUT),(?<to>\d*)(:?L|l)?)|((?:["']materializeProperties["']|(?:Tokens\.)?ARGS_MATERIALIZE_PROPERTIES),["'](?<mp>.*?)["']?)|((?:["']requestId["']|(?:Tokens\.)?REQUEST_ID),["'](?<rid>.*?)["']))
     * 1st Alternative ((?:["']materializeProperties["']|["']scriptEvaluationTimeout["']|(?:Tokens\.)?ARGS_EVAL_TIMEOUT|(?:Tokens\.)?ARGS_SCRIPT_EVAL_TIMEOUT),(?<to>\d*)(:?L|l)?)
     * 2nd Capturing Group ((?:["']materializeProperties["']|["']scriptEvaluationTimeout["']|(?:Tokens\.)?ARGS_EVAL_TIMEOUT|(?:Tokens\.)?ARGS_SCRIPT_EVAL_TIMEOUT),(?<to>\d*)(:?L|l)?)
     * Non-capturing group (?:["']materializeProperties["']|["']scriptEvaluationTimeout["']|(?:Tokens\.)?ARGS_EVAL_TIMEOUT|(?:Tokens\.)?ARGS_SCRIPT_EVAL_TIMEOUT)
     * 1st Alternative ["']materializeProperties["']
     * Match a single character present in the list below ["']
     * "' matches a single character in the list "' (case sensitive)
     * materializeProperties
     *  matches the characters materializeProperties literally (case sensitive)
     * Match a single character present in the list below ["']
     * "' matches a single character in the list "' (case sensitive)
     * 2nd Alternative ["']scriptEvaluationTimeout["']
     * Match a single character present in the list below ["']
     * "' matches a single character in the list "' (case sensitive)
     * scriptEvaluationTimeout matches the characters scriptEvaluationTimeout literally (case sensitive)
     * Match a single character present in the list below ["']
     * "' matches a single character in the list "' (case sensitive)
     * 3rd Alternative (?:Tokens\.)?ARGS_EVAL_TIMEOUT
     * Non-capturing group (?:Tokens\.)?
     * ? matches the previous token between zero and one times, as many times as possible, giving back as needed (greedy)
     * Tokens matches the characters Tokens literally (case sensitive)
     * \. matches the character . with index 4610 (2E16 or 568) literally (case sensitive)
     * ARGS_EVAL_TIMEOUT matches the characters ARGS_EVAL_TIMEOUT literally (case sensitive)
     * 4th Alternative (?:Tokens\.)?ARGS_SCRIPT_EVAL_TIMEOUT
     * Non-capturing group (?:Tokens\.)?
     * ? matches the previous token between zero and one times, as many times as possible, giving back as needed (greedy)
     * Tokens matches the characters Tokens literally (case sensitive)
     * \. matches the character . with index 4610 (2E16 or 568) literally (case sensitive)
     * ARGS_SCRIPT_EVAL_TIMEOUT matches the characters ARGS_SCRIPT_EVAL_TIMEOUT literally (case sensitive)
     * , matches the character , with index 4410 (2C16 or 548) literally (case sensitive)
     * Named Capture Group to (?<to>\d*)
     * \d matches a digit (equivalent to [0-9])
     * * matches the previous token between zero and unlimited times, as many times as possible, giving back as needed (greedy)
     * 4th Capturing Group (:?L|l)?
     * ? matches the previous token between zero and one times, as many times as possible, giving back as needed (greedy)
     * 1st Alternative :?L
     * : matches the character : with index 5810 (3A16 or 728) literally (case sensitive)
     * ? matches the previous token between zero and one times, as many times as possible, giving back as needed (greedy)
     * L matches the character L with index 7610 (4C16 or 1148) literally (case sensitive)
     * 2nd Alternative l
     * l matches the character l with index 10810 (6C16 or 1548) literally (case sensitive)
     * 2nd Alternative ((?:["']materializeProperties["']|(?:Tokens\.)?ARGS_MATERIALIZE_PROPERTIES),["'](?<mp>.*?)["']?)
     * 5th Capturing Group ((?:["']materializeProperties["']|(?:Tokens\.)?ARGS_MATERIALIZE_PROPERTIES),["'](?<mp>.*?)["']?)
     * Non-capturing group (?:["']materializeProperties["']|(?:Tokens\.)?ARGS_MATERIALIZE_PROPERTIES)
     * 1st Alternative ["']materializeProperties["']
     * 2nd Alternative (?:Tokens\.)?ARGS_MATERIALIZE_PROPERTIES
     * , matches the character , with index 4410 (2C16 or 548) literally (case sensitive)
     * Match a single character present in the list below ["']
     * "' matches a single character in the list "' (case sensitive)
     * Named Capture Group mp (?<mp>.*?)
     * Match a single character present in the list below ["']
     * 3rd Alternative ((?:["']requestId["']|(?:Tokens\.)?REQUEST_ID),["'](?<rid>.*?)["'])
     * 7th Capturing Group ((?:["']requestId["']|(?:Tokens\.)?REQUEST_ID),["'](?<rid>.*?)["'])
     * \) matches the character ) with index 4110 (2916 or 518) literally (case sensitive)
     * Global pattern flags
     * g modifier: global. All matches (don't return after first match)
     * m modifier: multi line. Causes ^ and $ to match the begin/end of each line (not only begin/end of string)
     * </pre>
     */
    private static final Pattern patternWithOptions =
            Pattern.compile("\\.with\\((((?:"
                    + timeoutTokens + "),(?<to>\\d*)(:?L|l)?)|((?:"
                    + materializePropertiesTokens + "),[\"'](?<mp>.*?)[\"']?)|((?:"
                    + languageTokens + "),[\"'](?<lang>.*?)[\"'])|((?:"
                    + batchSizeTokens + "),(?<bs>\\d+))|((?:"
                    + bulkResultsTokens + "),(?<br>true|false)))\\)");

    /**
     * Parses a Gremlin script and extracts a {@code Result} containing properties that are relevant to the checker.
     */
    public static Result parse(final String gremlin) {
        if (gremlin.isEmpty()) return EMPTY_RESULT;

        // do a cheap check for tokens we care about - no need to parse unless one of these tokens is present in
        // the string.
        if (tokens.stream().noneMatch(gremlin::contains)) return EMPTY_RESULT;

        // kill out comments/whitespace. for whitespace, ignoring the need to keep string literals together as that
        // isn't currently a requirement
        final String cleanGremlin = patternClean.matcher(gremlin).replaceAll("");

        final Matcher m = patternWithOptions.matcher(cleanGremlin);
        if (!m.find()) return EMPTY_RESULT;

        // arguments given to Result class as null mean they weren't assigned (or the parser didn't find them somehow - eek!)
        Long timeout = null;
        String materializeProperties = null;
        String language = null;
        String batchSize = null;
        Boolean bulkResults = null;
        do {
            // timeout is added up across all scripts
            final String to = m.group("to");
            if (to != null) {
                if (null == timeout) timeout = 0L;
                timeout += Long.parseLong(to);
            }

            //materializeProperties just uses the last one found
            final String mp = m.group("mp");
            if (mp != null) materializeProperties = mp;

            // language just uses the last one found
            final String lang = m.group("lang");
            if (lang != null) language = lang;

            // batchSize just uses the last one found. it is captured as the raw string (not parsed to a number) so
            // that an out-of-range value does not throw here in the Context constructor - the server parses and
            // validates it where a bad value can be surfaced as a bad request rather than an uncaught error.
            final String bs = m.group("bs");
            if (bs != null) batchSize = bs;

            // bulkResults just uses the last one found
            final String br = m.group("br");
            if (br != null) bulkResults = Boolean.parseBoolean(br);
        } while (m.find());

        return new Result(timeout, materializeProperties, language, batchSize, bulkResults);
    }

    /**
     * A result returned from a {@link #parse(String)} of a Gremlin string.
     */
    public static class Result {
        private final Long timeout;
        private final String materializeProperties;
        private final String language;
        private final String batchSize;
        private final Boolean bulkResults;

        private Result(final Long timeout, final String materializeProperties,
                       final String language, final String batchSize, final Boolean bulkResults) {
            this.timeout = timeout;
            this.materializeProperties = materializeProperties;
            this.language = language;
            this.batchSize = batchSize;
            this.bulkResults = bulkResults;
        }

        /**
         * Gets the value of the timeouts that were set using the {@link GraphTraversal#with(String, Object)} source step.
         * If there are multiple commands using this step, the timeouts are summed together.
         */
        public final Optional<Long> getTimeout() {
            return null == timeout ? Optional.empty() : Optional.of(timeout);
        }

        /**
         * Gets the value of the materializeProperties supplied using the {@link GraphTraversal#with(String, Object)} source step.
         * If there are multiple commands using this step, the last usage should represent the value returned here.
         */
        public Optional<String> getMaterializeProperties() {
            return null == materializeProperties ? Optional.empty() : Optional.of(materializeProperties);
        }

        /**
         * Gets the value of the language supplied using the {@link GraphTraversal#with(String, Object)} source step.
         * If there are multiple commands using this step, the last usage should represent the value returned here.
         */
        public Optional<String> getLanguage() {
            return null == language ? Optional.empty() : Optional.of(language);
        }

        /**
         * Gets the raw, unparsed value of the batchSize supplied using the {@link GraphTraversal#with(String, Object)}
         * source step. It is returned as the raw string (rather than a parsed number) so that parsing and range
         * validation happen where an invalid value can be surfaced as a bad request rather than an uncaught error.
         * If there are multiple commands using this step, the last usage should represent the value returned here.
         */
        public Optional<String> getBatchSize() {
            return null == batchSize ? Optional.empty() : Optional.of(batchSize);
        }

        /**
         * Gets the value of the bulkResults flag supplied using the {@link GraphTraversal#with(String, Object)} source step.
         * If there are multiple commands using this step, the last usage should represent the value returned here.
         */
        public Optional<Boolean> getBulkResults() {
            return null == bulkResults ? Optional.empty() : Optional.of(bulkResults);
        }

        @Override
        public String toString() {
            return "GremlinScriptChecker.Result{" +
                    "timeout=" + timeout +
                    ", materializeProperties='" + materializeProperties + '\'' +
                    ", language='" + language + '\'' +
                    ", batchSize=" + batchSize +
                    ", bulkResults=" + bulkResults +
                    '}';
        }
    }
}
