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

import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO

public class Preferences {

    private static final java.util.prefs.Preferences STORE = java.util.prefs.Preferences.userRoot().node("/org/apache/tinkerpop/gremlin/console");

    public static final String PREFERENCE_ITERATION_MAX = "max-iteration"
    private static final int DEFAULT_ITERATION_MAX = 100
    public static int maxIteration = DEFAULT_ITERATION_MAX

    public static final String PREF_GREMLIN_COLOR = "gremlin.color"
    public static final String PREF_GREMLIN_COLOR_DEFAULT = "reset"
    public static String gremlinColor = PREF_GREMLIN_COLOR_DEFAULT

    public static final String PREF_VERTEX_COLOR = "vertex.color"
    public static final String PREF_VERTEX_COLOR_DEFAULT = "reset"
    public static String  vertexColor = PREF_VERTEX_COLOR_DEFAULT

    public static final String PREF_EDGE_COLOR = "edge.color"
    public static final String PREF_EDGE_COLOR_DEFAULT = "reset"
    public static String  edgeColor = PREF_EDGE_COLOR_DEFAULT

    public static final String PREF_ERROR_COLOR = "error.color"
    public static final String PREF_ERROR_COLOR_DEFAULT = "reset"
    public static String  errorColor = PREF_ERROR_COLOR_DEFAULT

    public static final String PREF_INFO_COLOR = "info.color"
    public static final String PREF_INFO_COLOR_DEFAULT = "reset"
    public static String  infoColor = PREF_INFO_COLOR_DEFAULT

    public static final String PREF_STRING_COLOR = "string.color"
    public static final String PREF_STRING_COLOR_DEFAULT = "reset"
    public static String  stringColor = PREF_STRING_COLOR_DEFAULT

    public static final String PREF_NUMBER_COLOR = "number.color"
    public static final String PREF_NUMBER_COLOR_DEFAULT = "reset"
    public static String  numberColor = PREF_NUMBER_COLOR_DEFAULT

    public static final String PREF_T_COLOR = "T.color"
    public static final String PREF_T_COLOR_DEFAULT = "reset"
    public static String  tColor = PREF_T_COLOR_DEFAULT

    public static final String PREF_INPUT_PROMPT_COLOR = "input.prompt.color"
    public static final String PREF_INPUT_PROMPT_COLOR_DEFAULT = "reset"
    public static String  inputPromptColor = PREF_INPUT_PROMPT_COLOR_DEFAULT

    public static final String PREF_RESULT_PROMPT_COLOR = "result.prompt.color"
    public static final String PREF_RESULT_PROMPT_COLOR_DEFAULT = "reset"
    public static String  resultPromptColor = PREF_RESULT_PROMPT_COLOR_DEFAULT

    public static final String PREF_EMPTY_RESULT_IND = "empty.result.indicator"
    public static final String PREF_EMPTY_RESULT_IND_DEFAULT = "null"
    public static String  emptyResult = PREF_EMPTY_RESULT_IND_DEFAULT

    public static final String PREF_INPUT_PROMPT = "input.prompt"
    public static final String PREF_INPUT_PROMPT_DEFAULT = "gremlin>"
    public static String  inputPrompt = PREF_INPUT_PROMPT_DEFAULT

    public static final String PREF_RESULT_PROMPT = "result.prompt"
    public static final String PREF_RESULT_PROMPT_DEFAULT = "==>"
    public static String  resultPrompt = PREF_RESULT_PROMPT_DEFAULT

    public static final String PREF_COLORS = Groovysh.COLORS_PREFERENCE_KEY
    public static final Boolean PREF_COLORS_DEFAULT = true;
    public static boolean colors = PREF_COLORS_DEFAULT

    public static void expandoMagic() {

        // Override all GroovySH Preference methods
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

        // Gremlin Handlers

        // Initial Load
        loadDefaultValues()

        // Listeners
        installPropertyListeners()
    }

    private static installPropertyListeners() {
        org.codehaus.groovy.tools.shell.util.Preferences.addChangeListener(new PreferenceChangeListener() {
                    @Override
                    void preferenceChange(PreferenceChangeEvent evt) {
                        if (evt.key == PREFERENCE_ITERATION_MAX && null != evt.newValue) {
                            try {
                                maxIteration = Integer.parseInt(evt.newValue)
                            } catch (NumberFormatException e) {
                                println(Colorizer.render(errorColor,"Unable to convert '${evt.newValue}' to integer. Using default ${DEFAULT_ITERATION_MAX}"))
                                maxIteration = DEFAULT_ITERATION_MAX
                            }
                        } else if (evt.key == PREFERENCE_ITERATION_MAX){
                            maxIteration = DEFAULT_ITERATION_MAX
                        }
                    }
                })

        org.codehaus.groovy.tools.shell.util.Preferences.addChangeListener(new PreferenceChangeListener() {
                    @Override
                    void preferenceChange(PreferenceChangeEvent evt) {
                        if (evt.key == PREF_GREMLIN_COLOR) {
                            if (null == evt.newValue) {
                                gremlinColor = STORE.get(PREF_GREMLIN_COLOR, PREF_GREMLIN_COLOR_DEFAULT)
                            } else {
                                gremlinColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_VERTEX_COLOR) {
                            if (null == evt.newValue) {
                                vertexColor =  STORE.get(PREF_VERTEX_COLOR, PREF_VERTEX_COLOR_DEFAULT)
                            } else {
                                vertexColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_EDGE_COLOR) {
                            if (null == evt.newValue) {
                                edgeColor =  STORE.get(PREF_EDGE_COLOR, PREF_EDGE_COLOR_DEFAULT)
                            } else {
                                edgeColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_ERROR_COLOR) {
                            if (null == evt.newValue) {
                                errorColor =  STORE.get(PREF_ERROR_COLOR, PREF_ERROR_COLOR_DEFAULT)
                            } else {
                                errorColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_INFO_COLOR) {
                            if (null == evt.newValue) {
                                infoColor =  STORE.get(PREF_INFO_COLOR, PREF_INFO_COLOR_DEFAULT)
                            } else {
                                infoColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_STRING_COLOR) {
                            if (null == evt.newValue) {
                                stringColor =  STORE.get(PREF_STRING_COLOR, PREF_STRING_COLOR_DEFAULT)
                            } else {
                                stringColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_NUMBER_COLOR) {
                            if (null == evt.newValue) {
                                numberColor =  STORE.get(PREF_NUMBER_COLOR, PREF_NUMBER_COLOR_DEFAULT)
                            } else {
                                numberColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_T_COLOR) {
                            if (null == evt.newValue) {
                                tColor =  STORE.get(PREF_T_COLOR, PREF_T_COLOR_DEFAULT)
                            } else {
                                tColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_INPUT_PROMPT_COLOR) {
                            if (null == evt.newValue) {
                                inputPromptColor =  STORE.get(PREF_INPUT_PROMPT_COLOR, PREF_INPUT_PROMPT_COLOR_DEFAULT)
                            } else {
                                inputPromptColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_RESULT_PROMPT_COLOR) {
                            if (null == evt.newValue) {
                                resultPromptColor =  STORE.get(PREF_RESULT_PROMPT_COLOR, PREF_RESULT_PROMPT_COLOR_DEFAULT)
                            } else {
                                resultPromptColor = evt.newValue
                            }
                        } else   if (evt.key == PREF_EMPTY_RESULT_IND) {
                            if (null == evt.newValue) {
                                emptyResult =  STORE.get(PREF_EMPTY_RESULT_IND, PREF_EMPTY_RESULT_IND_DEFAULT)
                            } else {
                                emptyResult = evt.newValue
                            }
                        } else   if (evt.key == PREF_INPUT_PROMPT) {
                            if (null == evt.newValue) {
                                inputPrompt =  STORE.get(PREF_INPUT_PROMPT, PREF_INPUT_PROMPT_DEFAULT)
                            } else {
                                inputPrompt = evt.newValue
                            }
                        } else   if (evt.key == PREF_RESULT_PROMPT) {
                            if (null == evt.newValue) {
                                resultPrompt =  STORE.get(PREF_RESULT_PROMPT, PREF_RESULT_PROMPT_DEFAULT)
                            } else {
                                resultPrompt = evt.newValue
                            }
                        }  else   if (evt.key == PREF_COLORS) {
                            if (null == evt.newValue) {
                                colors =  Boolean.valueOf(STORE.get(PREF_COLORS, PREF_COLORS_DEFAULT.toString()))
                            } else {
                                colors = Boolean.valueOf(evt.newValue)
                            }
                        }
                    }
                })

    }

    private static loadDefaultValues() {
        try {
            maxIteration = STORE.get(PREFERENCE_ITERATION_MAX, DEFAULT_ITERATION_MAX.toString()).toInteger()
        }catch (NumberFormatException e) {
            String maxIterationString = STORE.get(PREFERENCE_ITERATION_MAX, DEFAULT_ITERATION_MAX.toString())
            println(Colorizer.render(Preferences.errorColor,"Unable to convert '${maxIterationString}' to integer. Using default ${DEFAULT_ITERATION_MAX}"))
            maxIteration = DEFAULT_ITERATION_MAX
        }

        gremlinColor = STORE.get(PREF_GREMLIN_COLOR, PREF_GREMLIN_COLOR_DEFAULT)

        vertexColor =  STORE.get(PREF_VERTEX_COLOR, PREF_VERTEX_COLOR_DEFAULT)

        edgeColor =  STORE.get(PREF_EDGE_COLOR, PREF_EDGE_COLOR_DEFAULT)

        errorColor =  STORE.get(PREF_ERROR_COLOR, PREF_ERROR_COLOR_DEFAULT)

        infoColor =  STORE.get(PREF_INFO_COLOR, PREF_INFO_COLOR_DEFAULT)

        stringColor =  STORE.get(PREF_STRING_COLOR, PREF_STRING_COLOR_DEFAULT)

        numberColor =  STORE.get(PREF_NUMBER_COLOR, PREF_NUMBER_COLOR_DEFAULT)

        tColor =  STORE.get(PREF_T_COLOR, PREF_T_COLOR_DEFAULT)

        inputPromptColor =  STORE.get(PREF_INPUT_PROMPT_COLOR, PREF_INPUT_PROMPT_COLOR_DEFAULT)

        resultPromptColor =  STORE.get(PREF_RESULT_PROMPT_COLOR, PREF_RESULT_PROMPT_COLOR_DEFAULT)

        emptyResult =  STORE.get(PREF_EMPTY_RESULT_IND, PREF_EMPTY_RESULT_IND_DEFAULT)

        inputPrompt =  STORE.get(PREF_INPUT_PROMPT, PREF_INPUT_PROMPT_DEFAULT)

        resultPrompt =  STORE.get(PREF_RESULT_PROMPT, PREF_RESULT_PROMPT_DEFAULT)

        colors =  Boolean.valueOf(STORE.get(PREF_COLORS, PREF_COLORS_DEFAULT.toString()))
    }
}
