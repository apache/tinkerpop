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
package org.apache.tinkerpop.gremlin.structure.util.batch.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class URLCompression implements StringCompression {

    private static final String DELIMITER = "$";

    private int prefixCounter = 0;

    private final Map<String, String> urlPrefix = new HashMap<>();

    @Override
    public String compress(final String input) {
        final String[] url = splitURL(input);
        String prefix = urlPrefix.get(url[0]);
        if (prefix == null) {
            //New Prefix
            prefix = Long.toString(prefixCounter, Character.MAX_RADIX) + DELIMITER;
            prefixCounter++;
            urlPrefix.put(url[0], prefix);
        }
        return prefix + url[1];
    }

    private final static char[] urlDelimiters = new char[]{'/', '#', ':'};

    private static String[] splitURL(final String url) {
        final String[] res = new String[2];
        int pos = -1;
        for (char delimiter : urlDelimiters) {
            int currentpos = url.lastIndexOf(delimiter);
            if (currentpos > pos) pos = currentpos;
        }
        if (pos < 0) {
            res[0] = "";
            res[1] = url;
        } else {
            res[0] = url.substring(0, pos + 1);
            res[1] = url.substring(pos + 1);
        }
        return res;
    }
}
