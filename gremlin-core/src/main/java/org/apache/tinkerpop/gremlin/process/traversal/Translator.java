/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal;

import java.util.function.BiFunction;

/**
 * A Translator will translate {@link Bytecode} into another representation. That representation may be a
 * Java instance via {@link StepTranslator} or a String script in some language via {@link ScriptTranslator}.
 * The parameterization of Translator is S (traversal source) and T (full translation).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Translator<S, T> {

    /**
     * Get the {@link TraversalSource} representation rooting this translator.
     * For string-based translators ({@link ScriptTranslator}), this is typically a "g".
     * For java-based translators ({@link StepTranslator}), this is typically the {@link TraversalSource} instance which the {@link Traversal} will be built from.
     *
     * @return the traversal source representation
     */
    public S getTraversalSource();

    /**
     * Translate {@link Bytecode} into a new representation.
     * Typically, for language translations, the translation is to a string represenging the traversal in the respective scripting language.
     *
     * @param bytecode the byte code representing traversal source and traversal manipulations.
     * @return the translated object
     */
    public T translate(final Bytecode bytecode);

    /**
     * Get the language that the translator is converting the traversal byte code to.
     *
     * @return the language of the translation
     */
    public String getTargetLanguage();

    ///

    /**
     * Translates bytecode to a string representation.
     */
    public interface ScriptTranslator extends Translator<String, String> {

        /**
         * Provides a way for the {@link ScriptTranslator} to convert various data types to their string
         * representations in their target language.
         */
        public interface TypeTranslator extends BiFunction<String, Object, Object> { }
    }

    /**
     * Translates bytecode to actual steps.
     */
    public interface StepTranslator<S extends TraversalSource, T extends Traversal.Admin<?, ?>> extends Translator<S, T> {

    }
}
