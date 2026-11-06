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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.util;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Parses a configuration value that holds a comma-separated string, a multi-valued property, or a single token into an
 * ordered set of trimmed, non-empty entries. Shared by the allow-lists that read such values so they interpret an
 * operator's lists identically: class names in {@link OlapClassLoadingPolicy} and configuration keys in
 * {@link OlapConfigKeyPolicy}. It is deliberately value-type-neutral -- it knows nothing about classes or keys -- so
 * neither of those depends on the other.
 * <p/>
 * Package-private: only the same-package allow-lists use it, so it is kept off the module's public API surface.
 */
final class ConfigValueParser {

    private ConfigValueParser() {
    }

    /**
     * Splits {@code value} into trimmed, non-empty entries, preserving order and de-duplicating. Accepts a
     * {@link Collection} (each element split on commas) or a single object whose {@code toString()} is split on commas;
     * {@code null} (and {@code null} elements) yield no entries.
     */
    static Set<String> parse(final Object value) {
        final Set<String> parsed = new LinkedHashSet<>();
        if (value instanceof Collection) {
            for (final Object entry : (Collection<?>) value)
                if (null != entry) addSplit(parsed, entry.toString());
        } else if (null != value) {
            addSplit(parsed, value.toString());
        }
        return parsed;
    }

    private static void addSplit(final Set<String> parsed, final String value) {
        for (final String part : value.split(",")) {
            final String trimmed = part.trim();
            if (!trimmed.isEmpty()) parsed.add(trimmed);
        }
    }
}
