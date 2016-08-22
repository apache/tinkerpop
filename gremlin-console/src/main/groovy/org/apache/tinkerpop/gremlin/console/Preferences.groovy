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
package org.apache.tinkerpop.gremlin.console;

import java.util.prefs.PreferenceChangeEvent
import java.util.prefs.PreferenceChangeListener

import org.codehaus.groovy.tools.shell.IO

public class Preferences {

    private static final java.util.prefs.Preferences STORE = java.util.prefs.Preferences.userRoot().node("/org/apache/tinkerpop/gremlin/console");

    public static void expandoMagic() {
        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.getShowLastResult = {
            return STORE.getBoolean(org.codehaus.groovy.tools.shell.util.Preferences.SHOW_LAST_RESULT_KEY, true);
        }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.getSanitizeStackTrace = {
            return STORE.getBoolean(org.codehaus.groovy.tools.shell.util.Preferences.SANITIZE_STACK_TRACE_KEY, true);
        }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.getEditor = {
            return STORE.get(org.codehaus.groovy.tools.shell.util.Preferences.EDITOR_KEY, System.getenv("EDITOR"));
        }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.getParserFlavor = {
            return STORE.get(org.codehaus.groovy.tools.shell.util.Preferences.PARSER_FLAVOR_KEY, org.codehaus.groovy.tools.shell.util.Preferences.PARSER_RIGID);
        }

        //
        // Store Access
        //

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.keys =  {
            return STORE.keys();
        }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.get = { String name, String defaultValue ->
            return STORE.get(name, defaultValue);
        }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.get = { String name ->
            return get(name, null);
        }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.put = { String name, String value ->
            STORE.put(name, value);
        }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.clear =  { STORE.clear(); }

        org.codehaus.groovy.tools.shell.util.Preferences.getMetaClass().'static'.addChangeListener = { PreferenceChangeListener listener ->
            STORE.addPreferenceChangeListener(listener);
        }

        // reinstall change handler
        String tmp = STORE.get(org.codehaus.groovy.tools.shell.util.Preferences.VERBOSITY_KEY, IO.Verbosity.INFO.name);
        try {
            org.codehaus.groovy.tools.shell.util.Preferences.verbosity = IO.Verbosity.forName(tmp);
        }
        catch (IllegalArgumentException e) {
            org.codehaus.groovy.tools.shell.util.Preferences.verbosity = IO.Verbosity.INFO;
            STORE.remove(org.codehaus.groovy.tools.shell.util.Preferences.VERBOSITY_KEY);
        }

        org.codehaus.groovy.tools.shell.util.Preferences.addChangeListener(new PreferenceChangeListener() {
                    public void preferenceChange(final PreferenceChangeEvent event) {
                        if (event.getKey().equals(org.codehaus.groovy.tools.shell.util.Preferences.VERBOSITY_KEY)) {
                            String name = event.getNewValue();

                            if (name == null) {
                                name = IO.Verbosity.INFO.name;
                            }

                            try {
                                org.codehaus.groovy.tools.shell.util.Preferences.verbosity = IO.Verbosity.forName(name);
                            }
                            catch (Exception e) {
                                event.getNode().put(event.getKey(), org.codehaus.groovy.tools.shell.util.Preferences.verbosity.name);
                            }
                        }
                    }
                });
    }
}
