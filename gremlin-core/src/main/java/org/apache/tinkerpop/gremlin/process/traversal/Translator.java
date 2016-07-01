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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Translator<V> {

    /**
     * Get the {@link TraversalSource} alias rooting this translator.
     *
     * @return the string alias (typically "g").
     */
    public String getAlias();

    /**
     * Translate {@link ByteCode} into a new representation.
     * Typically, for language translations, the translation is to a string represenging the traversal in the respective scripting language.
     *
     * @param byteCode the byte code representing traversal source and traversal manipulations.
     * @return the translated object
     */
    public V translate(final ByteCode byteCode);

    public String getSourceLanguage();

    /**
     * Get the language that the translator is converting the traversal byte code to.
     *
     * @return the language of the translation
     */
    public String getTargetLanguage();

    // TODO: getTranslationClass() -- e.g. for GroovyTranslator, its String.
    // TODO: Do we need getSourceLanguage()? The source language is always ByteCode now!
}
