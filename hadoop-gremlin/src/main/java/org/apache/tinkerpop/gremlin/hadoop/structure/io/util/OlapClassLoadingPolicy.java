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

import org.apache.commons.configuration2.Configuration;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The class-loading policy for untrusted OLAP IO: an allow-list of fully-qualified class names that IO code is
 * permitted to resolve and load when the class name originates from an untrusted source (for example, a value supplied
 * on a remote traversal), together with the trust flag ({@link #TRUSTED}) that governs whether the allow-list applies.
 * The allow-list is a plain membership check paired with a {@link #resolve(String, Class)} helper that never consults a
 * class loader for a name that has not been approved first. It is the class-loading counterpart to
 * {@link OlapConfigKeyPolicy}, which governs configuration keys.
 * <p/>
 * The policy itself carries no knowledge of which classes are "built in" or where names come from; callers assemble
 * one from whatever trusted sources apply to them &mdash; product built-ins, the operator-supplied
 * {@code gremlin.io.approvedClasses} configuration (see {@link Builder#approveFrom(Configuration)}), and any classes
 * already declared in a trusted graph configuration. This keeps the type module-neutral: modules such as
 * {@code hadoop-gremlin} and {@code spark-gremlin} contribute their own built-in class names rather than having them
 * hard-coded here.
 */
public final class OlapClassLoadingPolicy {

    /**
     * Configuration key holding a comma-separated list (or multi-valued property) of additional fully-qualified class
     * names an operator approves for resolution from untrusted IO.
     */
    public static final String APPROVED_CLASSES = "gremlin.io.approvedClasses";

    /**
     * Configuration key that opts a deployment into <em>trusted</em> IO, restoring full class loading (no allow-list
     * enforcement). It defaults to {@code false} (untrusted), so the allow-list applies unless an operator explicitly
     * turns it on. This flag must only ever be read from trusted, operator-controlled configuration &mdash; never from
     * per-request/traversal options &mdash; and callers that copy untrusted options into configuration must refuse to
     * copy this key, so an untrusted request cannot elevate itself.
     */
    public static final String TRUSTED = "gremlin.io.trusted";

    /**
     * Returns whether the given (trusted, operator-controlled) configuration opts into trusted IO. Absent or non-boolean
     * values are treated as {@code false} so IO is untrusted by default.
     */
    public static boolean isTrusted(final Configuration configuration) {
        return null != configuration && configuration.getBoolean(TRUSTED, false);
    }

    private final Set<String> approved;

    private OlapClassLoadingPolicy(final Set<String> approved) {
        this.approved = approved;
    }

    public static Builder build() {
        return new Builder();
    }

    /**
     * Returns {@code true} if the given fully-qualified class name has been approved.
     */
    public boolean isApproved(final String className) {
        return className != null && approved.contains(className);
    }

    /**
     * The unmodifiable set of approved fully-qualified class names.
     */
    public Set<String> approvedClasses() {
        return Collections.unmodifiableSet(approved);
    }

    /**
     * Resolves an approved class name to a {@link Class} assignable to {@code expectedType}. The approval check happens
     * <em>before</em> any class loader is consulted, so a name that is not on the allow-list is rejected without being
     * loaded (and therefore without running its static initializer).
     *
     * @throws IllegalArgumentException if the name is not approved, cannot be loaded, or is not assignable to
     *         {@code expectedType}
     */
    @SuppressWarnings("unchecked")
    public <T> Class<? extends T> resolve(final String className, final Class<T> expectedType) {
        if (!isApproved(className))
            throw new IllegalArgumentException(String.format(
                    "The class '%s' is not approved for resolution from untrusted IO. If it is trusted, add it to the '%s' configuration.",
                    className, APPROVED_CLASSES));

        final Class<?> clazz;
        try {
            // load without initializing: the assignability check below runs before any static initializer, so a class
            // that turns out not to be assignable is rejected without its initializer ever running
            clazz = Class.forName(className, false, OlapClassLoadingPolicy.class.getClassLoader());
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format("The approved class '%s' could not be loaded", className), e);
        }

        if (!expectedType.isAssignableFrom(clazz))
            throw new IllegalArgumentException(String.format(
                    "The approved class '%s' is not a '%s'", className, expectedType.getName()));

        return (Class<? extends T>) clazz;
    }

    public static final class Builder {

        private final Set<String> approved = new LinkedHashSet<>();

        private Builder() {
        }

        /**
         * Approves the given fully-qualified class names. {@code null} entries are ignored.
         */
        public Builder approve(final String... classNames) {
            if (null != classNames)
                for (final String className : classNames)
                    addTrimmed(className);
            return this;
        }

        /**
         * Approves the given classes by their fully-qualified names. {@code null} entries are ignored.
         */
        public Builder approve(final Class<?>... classes) {
            if (null != classes)
                for (final Class<?> clazz : classes)
                    if (null != clazz) addTrimmed(clazz.getName());
            return this;
        }

        /**
         * Approves the names listed under {@link #APPROVED_CLASSES} in the given configuration, if present. The value
         * may be a comma-separated {@link String} or a multi-valued property that resolves to a {@code Collection}.
         */
        public Builder approveFrom(final Configuration configuration) {
            if (null != configuration && configuration.containsKey(APPROVED_CLASSES))
                approveValue(configuration.getProperty(APPROVED_CLASSES));
            return this;
        }

        /**
         * Auto-seeds the registry from a trusted configuration by approving the class name(s) held under each of the
         * given keys, if present. This lets a deployment's already-declared classes (graph reader/writer, graph
         * computer, serializer, registrator, ...) be trusted by construction, so they resolve without an operator also
         * listing them under {@link #APPROVED_CLASSES}. Values may be single strings, comma-separated strings, or
         * multi-valued properties. The keys to read are supplied by the caller because they are module-specific
         * (gremlin-core does not know the Hadoop/Spark configuration keys).
         */
        public Builder approveFromConfigValues(final Configuration configuration, final String... keys) {
            if (null != configuration && null != keys)
                for (final String key : keys)
                    if (null != key && configuration.containsKey(key))
                        approveValue(configuration.getProperty(key));
            return this;
        }

        private void approveValue(final Object value) {
            approved.addAll(ConfigValueParser.parse(value));
        }

        private void addTrimmed(final String className) {
            if (null != className) {
                final String trimmed = className.trim();
                if (!trimmed.isEmpty()) approved.add(trimmed);
            }
        }

        public OlapClassLoadingPolicy create() {
            return new OlapClassLoadingPolicy(new LinkedHashSet<>(approved));
        }
    }
}
