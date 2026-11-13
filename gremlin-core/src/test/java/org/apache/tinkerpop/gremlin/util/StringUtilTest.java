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
package org.apache.tinkerpop.gremlin.util;

import javax.lang.model.SourceVersion;
import java.util.Arrays;
import org.apache.tinkerpop.gremlin.AssertHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Andrea Child
 */
public class StringUtilTest {

    @Test
    public void shouldBeUtilityClass() throws Exception {
        AssertHelper.assertIsUtilityClass(StringUtil.class);
    }

    /**
     * {@code StringUtil.isIdentifier()} exists so that {@code gremlin-core} doesn't need the {@code java.compiler}
     * module. This confirms it agrees with {@code javax.lang.model.SourceVersion.isIdentifier()} on common cases:
     * typical parameter names, names that must be rejected, and a couple of non-ASCII letters.
     */
    @Test
    public void shouldMatchSourceVersionIsIdentifierForCommonCases() {
        for (final String s : new String[]{"x", "name", "_x", "$x", "x1", "camelCase", "é", "éclair"}) {
            assertIdentifierAgreement(s, true);
        }

        for (final String s : new String[]{"", "1x", "a-b", "a b", "a.b"}) {
            assertIdentifierAgreement(s, false);
        }
    }

    private static void assertIdentifierAgreement(final String s, final boolean expected) {
        assertEquals(s, expected, SourceVersion.isIdentifier(s));
        assertEquals(s, expected, StringUtil.isIdentifier(s));
    }

    @Test
    public void shouldSplitBySeparatorAndNotContainSeparator() {
        assertEquals(Arrays.asList("h","llo"), StringUtil.split("hello", "e"));
    }

    @Test
    public void shouldNotBeSplitIfSeparatorNotPresentInString() {
        final String toSplit = "test";
        assertEquals(Arrays.asList(toSplit), StringUtil.split(toSplit, "x"));
    }

    @Test
    public void shouldSplitByCharacterIfSeparatorIsEmpty() {
        assertEquals(Arrays.asList("t","e","s","t"," ","1","2","3"), StringUtil.split("test 123", ""));
    }

    @Test
    public void shouldSplitByWhitespaceIfSeparatorIsNull() {
        assertEquals(Arrays.asList("test","123"), StringUtil.split("test 123", null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfStringToSplitIsNull() {
        StringUtil.split(null, "test");
    }
}