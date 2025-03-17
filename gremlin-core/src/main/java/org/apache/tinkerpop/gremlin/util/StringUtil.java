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
package org.apache.tinkerpop.gremlin.util;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for Strings.
 *
 * @author Andrea Child
 */
public final class StringUtil {

    private StringUtil() {
    }

    /**
     * Splits the provided string into a List of string by the given separator, which will not be included in the returned List.
     *
     * @param toSplit the string to split
     * @param separator the separator string to split by - empty will split into a List of characters
     * @return a List of parsed strings, not including the given separator
     */
    public static List<String> split(final String toSplit, final String separator) {
        if (toSplit == null) {
            throw new IllegalArgumentException("toSplit cannot be null");
        }
        if (StringUtils.EMPTY.equals(separator)) {
            // split into a list of character strings
            // ie. "ab cd" -> ["a","b"," ","c","d"]
            return Arrays.asList(toSplit.split(StringUtils.EMPTY));
        }
        return Arrays.asList(StringUtils.splitByWholeSeparator(toSplit, separator));
    }
}
