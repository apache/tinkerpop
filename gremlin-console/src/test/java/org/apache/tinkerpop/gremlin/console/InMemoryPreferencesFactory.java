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
package org.apache.tinkerpop.gremlin.console;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.prefs.AbstractPreferences;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import java.util.prefs.PreferencesFactory;

/**
 * A purely in-memory {@link PreferencesFactory} used only by tests. Selected for the test JVM via the
 * {@code java.util.prefs.PreferencesFactory} system property (configured in this module's surefire setup) so that
 * console preference tests never read from or write to the developer's real per-user {@code java.util.prefs} store.
 * The property is resolved by {@link Preferences} on first access, before {@code Preferences.STORE} binds, so the
 * console's preference node is backed by this in-memory store for the entire fork.
 */
public class InMemoryPreferencesFactory implements PreferencesFactory {

    private static final Preferences USER_ROOT = new InMemoryPreferences(null, "");
    private static final Preferences SYSTEM_ROOT = new InMemoryPreferences(null, "");

    @Override
    public Preferences userRoot() {
        return USER_ROOT;
    }

    @Override
    public Preferences systemRoot() {
        return SYSTEM_ROOT;
    }

    /**
     * Minimal {@link AbstractPreferences} node keeping keys and children in memory. {@code AbstractPreferences}
     * handles locking and the asynchronous change-event dispatch, so the SPI methods only manage the maps.
     */
    private static final class InMemoryPreferences extends AbstractPreferences {

        private final Map<String, String> values = new ConcurrentHashMap<>();
        private final Map<String, InMemoryPreferences> children = new ConcurrentHashMap<>();

        InMemoryPreferences(final InMemoryPreferences parent, final String name) {
            super(parent, name);
        }

        @Override
        protected void putSpi(final String key, final String value) {
            values.put(key, value);
        }

        @Override
        protected String getSpi(final String key) {
            return values.get(key);
        }

        @Override
        protected void removeSpi(final String key) {
            values.remove(key);
        }

        @Override
        protected void removeNodeSpi() throws BackingStoreException {
            values.clear();
            children.clear();
        }

        @Override
        protected String[] keysSpi() throws BackingStoreException {
            return values.keySet().toArray(new String[0]);
        }

        @Override
        protected String[] childrenNamesSpi() throws BackingStoreException {
            return children.keySet().toArray(new String[0]);
        }

        @Override
        protected AbstractPreferences childSpi(final String name) {
            return children.computeIfAbsent(name, n -> new InMemoryPreferences(this, n));
        }

        @Override
        protected void syncSpi() throws BackingStoreException {
            // nothing to sync for an in-memory store
        }

        @Override
        protected void flushSpi() throws BackingStoreException {
            // nothing to flush for an in-memory store
        }
    }
}
