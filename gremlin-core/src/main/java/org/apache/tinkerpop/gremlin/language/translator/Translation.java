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
package org.apache.tinkerpop.gremlin.language.translator;

import java.util.Collections;
import java.util.Set;

/**
 * A translation of a script.
 */
public class Translation {

    private final String original;

    private final String translated;

    private final Set<String> parameters;

    Translation(final String original, final String translated, final Set<String> parameters) {
        this.original = original;
        this.translated = translated;
        this.parameters = parameters;
    }

    /**
     * Gets the original script.
     */
    public String getOriginal() {
        return original;
    }

    /**
     * Gets the translated script.
     */
    public String getTranslated() {
        return translated;
    }

    /**
     * Gets the parameters used in the translated script.
     */
    public Set<String> getParameters() {
        return Collections.unmodifiableSet(parameters);
    }

    @Override
    public String toString() {
        return translated;
    }
}
