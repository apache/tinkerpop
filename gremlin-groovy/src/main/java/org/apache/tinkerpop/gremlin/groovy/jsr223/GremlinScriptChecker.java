/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Processes Gremlin strings using regex so as to try to detect certain properties from the script without actual
 * having to execute a {@code eval()} on it.
 */
public class GremlinScriptChecker {

    public static final Result EMPTY_RESULT = new Result(0);
    private static final List<String> tokens = Arrays.asList("evaluationTimeout", "scriptEvaluationTimeout",
                                                             "ARGS_EVAL_TIMEOUT", "ARGS_SCRIPT_EVAL_TIMEOUT");

    /**
     * Matches single line comments, multi-line comments and space characters.
     * <pre>
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
     * Regex fragment for the timeout tokens to look for. There are basically four:
     * <ul>
     *     <li>{@code evaluationTimeout} which is a string value and thus single or double quoted</li>
     *     <li>{@code scriptEvaluationTimeout} which is a string value and thus single or double quoted</li>
     *     <li>{@code ARGS_EVAL_TIMEOUT} which is a enum type of value which can be referenced with or without a {@code Tokens} qualifier</li>
     *     <li>{@code ARGS_SCRIPT_EVAL_TIMEOUT} which is a enum type of value which can be referenced with or without a {@code Tokens} qualifier</li>
     * </ul>
     * <pre>
     * 	OR: match either of the followings
     * Sequence: match all of the followings in order
     * AnyCharIn[ " ']
     * e v a l u a t i o n T i m e o u t
     * AnyCharIn[ " ']
     * Sequence: match all of the followings in order
     * AnyCharIn[ " ']
     * s c r i p t E v a l u a t i o n T i m e o u t
     * AnyCharIn[ " ']
     * Sequence: match all of the followings in order
     * Repeat
     * CapturingGroup
     * (NonCapturingGroup)
     * Sequence: match all of the followings in order
     * T o k e n s
     * .
     * optional
     * A R G S _ E V A L _ T I M E O U T
     * Sequence: match all of the followings in order
     * Repeat
     * CapturingGroup
     * (NonCapturingGroup)
     * Sequence: match all of the followings in order
     * T o k e n s
     * .
     * optional
     * A R G S _ S C R I P T _ E V A L _ T I M E O U T
     * </pre>
     */
    private static final String timeoutTokens = "[\"']evaluationTimeout[\"']|[\"']scriptEvaluationTimeout[\"']|(?:Tokens\\.)?ARGS_EVAL_TIMEOUT|(?:Tokens\\.)?ARGS_SCRIPT_EVAL_TIMEOUT";

    /**
     * Matches {@code .with({timeout-token},{timeout})} with a matching group on the {@code timeout}.
     *
     * <pre>
     * Sequence: match all of the followings in order
     * .
     * w i t h
     * (
     * CapturingGroup
     * (NonCapturingGroup)
     * OR: match either of the followings
     * Sequence: match all of the followings in order
     * AnyCharIn[ " ']
     * e v a l u a t i o n T i m e o u t
     * AnyCharIn[ " ']
     * Sequence: match all of the followings in order
     * AnyCharIn[ " ']
     * s c r i p t E v a l u a t i o n T i m e o u t
     * AnyCharIn[ " ']
     * Sequence: match all of the followings in order
     * Repeat
     * CapturingGroup
     * (NonCapturingGroup)
     * Sequence: match all of the followings in order
     * T o k e n s
     * .
     * optional
     * A R G S _ E V A L _ T I M E O U T
     * Sequence: match all of the followings in order
     * Repeat
     * CapturingGroup
     * (NonCapturingGroup)
     * Sequence: match all of the followings in order
     * T o k e n s
     * .
     * optional
     * A R G S _ S C R I P T _ E V A L _ T I M E O U T
     * ,
     * CapturingGroup
     * GroupNumber:1
     * Repeat
     * Digit
     * zero or more times
     * Repeat
     * CapturingGroup
     * GroupNumber:2
     * OR: match either of the followings
     * Sequence: match all of the followings in order
     * Repeat
     * :
     * optional
     * L
     * l
     * optional
     * )
     * </pre>
     */
    private static final Pattern patternTimeout = Pattern.compile("\\.with\\((?:" + timeoutTokens + "),(\\d*)(:?L|l)?\\)");

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

        final Matcher m = patternTimeout.matcher(cleanGremlin);
        if (!m.find()) return EMPTY_RESULT;

        long l = Long.parseLong(m.group(1));
        while (m.find()) {
            l += Long.parseLong(m.group(1));
        }

        return new Result(l);
    }

    public static class Result {
        private final long timeout;

        private Result(final long timeout) {
            this.timeout = timeout;
        }

        /**
         * Gets the value of the timeouts that were set using the {@code with()} source step. If there are multiple
         * commands using this step, the timeouts are summed together.
         */
        public final Optional<Long> getTimeout() {
            return timeout == 0 ? Optional.empty() : Optional.of(timeout);
        }
    }
}
