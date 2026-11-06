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
import java.util.Set;

/**
 * Decides whether an OLAP configuration key/value may be set from an untrusted traversal. It is shared by the two
 * distinct surfaces that copy caller-supplied options into graph/computer configuration &mdash; a graph computer's
 * {@code configure(String, Object)} and an {@code io().with(String, Object)} step &mdash; so both behave the same way,
 * while each keeps its <em>own</em> operator allow-list (a computer-config key set is a different operator decision than
 * a graph/IO key set, so approving a key on one surface must not open it on the other).
 * <p/>
 * The policy is deny-by-default and, for an untrusted deployment, permits only a caller's built-in framework keys or the
 * keys the operator approved via the supplied allow-list configuration key. Trust and the allow-lists are read only from
 * the operator-controlled graph configuration, never from the key/value being set. The keys that govern the trust
 * boundary itself ({@link OlapClassLoadingPolicy#TRUSTED}, {@link OlapClassLoadingPolicy#APPROVED_CLASSES}, and the two
 * approved-config-key lists) are never settable through either surface &mdash; even in a trusted deployment &mdash; so a
 * traversal cannot elevate its own trust.
 */
public final class OlapConfigKeyPolicy {

    /**
     * Configuration key holding a comma-separated allow-list of computer-configuration keys an operator permits to be
     * set from an untrusted traversal via a graph computer's {@code configure(String, Object)}.
     */
    public static final String APPROVED_COMPUTER_CONFIG_KEYS = "gremlin.io.approvedComputerConfigKeys";

    /**
     * Configuration key holding a comma-separated allow-list of graph/IO configuration keys an operator permits to be
     * set from an untrusted traversal via {@code io().with(String, Object)}. Separate from
     * {@link #APPROVED_COMPUTER_CONFIG_KEYS} because {@code io().with()} and {@code configure()} govern different
     * surfaces.
     */
    public static final String APPROVED_GRAPH_CONFIG_KEYS = "gremlin.io.approvedGraphConfigKeys";

    private OlapConfigKeyPolicy() {
    }

    /**
     * Returns whether the given key governs the IO trust boundary and therefore may never be set through
     * {@code configure()} or {@code io().with()} (regardless of trust or allow-list), so a traversal cannot grant
     * itself trust.
     */
    public static boolean isMetaKey(final String key) {
        return OlapClassLoadingPolicy.TRUSTED.equals(key)
                || OlapClassLoadingPolicy.APPROVED_CLASSES.equals(key)
                || APPROVED_COMPUTER_CONFIG_KEYS.equals(key)
                || APPROVED_GRAPH_CONFIG_KEYS.equals(key);
    }

    /**
     * Returns whether {@code key} may be set from an untrusted traversal: never for a {@link #isMetaKey(String) meta
     * key}; always for a trusted deployment; otherwise only if it is one of {@code builtinKeys} or is listed under
     * {@code approvedKeysConfigKey} in the (operator-controlled) graph configuration.
     */
    public static boolean isConfigKeyPermitted(final Configuration graphConfiguration, final String key,
                                               final Set<String> builtinKeys, final String approvedKeysConfigKey) {
        if (isMetaKey(key))
            return false;
        if (OlapClassLoadingPolicy.isTrusted(graphConfiguration))
            return true;
        return (null != builtinKeys && builtinKeys.contains(key))
                || approvedKeys(graphConfiguration, approvedKeysConfigKey).contains(key);
    }

    /**
     * Enforces {@link #isConfigKeyPermitted}, throwing {@link IllegalArgumentException} with a clear message when the
     * key is a meta key (self-elevation) or is not permitted for an untrusted traversal.
     */
    public static void checkConfigKeyPermitted(final Configuration graphConfiguration, final String key,
                                               final Set<String> builtinKeys, final String approvedKeysConfigKey) {
        if (isMetaKey(key))
            throw new IllegalArgumentException(String.format(
                    "The configuration key '%s' governs the IO trust boundary and cannot be set via configure() or io().with().",
                    key));
        if (!isConfigKeyPermitted(graphConfiguration, key, builtinKeys, approvedKeysConfigKey))
            throw new IllegalArgumentException(String.format(
                    "The OLAP configuration key '%s' is not permitted for an untrusted traversal. Approve it via '%s' in " +
                    "trusted graph configuration, or set '%s' to true for a trusted deployment.",
                    key, approvedKeysConfigKey, OlapClassLoadingPolicy.TRUSTED));
    }

    /**
     * Reads the operator-approved key names under {@code approvedKeysConfigKey} via the shared
     * {@link ConfigValueParser} (so an operator's lists are parsed identically to class-name lists), while staying
     * independent of {@link OlapClassLoadingPolicy}'s policy: these are configuration <em>keys</em>, not class names.
     */
    private static Set<String> approvedKeys(final Configuration graphConfiguration, final String approvedKeysConfigKey) {
        if (null != graphConfiguration && null != approvedKeysConfigKey
                && graphConfiguration.containsKey(approvedKeysConfigKey))
            return ConfigValueParser.parse(graphConfiguration.getProperty(approvedKeysConfigKey));
        return Collections.emptySet();
    }
}
