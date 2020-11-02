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

import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * A Translator will translate {@link Bytecode} into another representation. That representation may be a
 * Java instance via {@link StepTranslator} or a String script in some language via {@link ScriptTranslator}.
 * The parameterization of Translator is S (traversal source) and T (full translation).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stark Arya (sandszhou.zj@alibaba-inc.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
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
     * Translates bytecode to a Script representation.
     */
    public interface ScriptTranslator extends Translator<String, Script> {

        /**
         * Provides a way for the {@link ScriptTranslator} to convert various data types to their string
         * representations in their target language.
         */
        public interface TypeTranslator extends BiFunction<String, Object, Script> { }

        public abstract class AbstractTypeTranslator implements ScriptTranslator.TypeTranslator {
            protected static final String ANONYMOUS_TRAVERSAL_PREFIX = "__";
            protected final boolean withParameters;
            protected final Script script;

            protected AbstractTypeTranslator(final boolean withParameters) {
                this.withParameters = withParameters;
                this.script = new Script();
            }

            @Override
            public Script apply(final String traversalSource, final Object o) {
                this.script.init();
                if (o instanceof Bytecode) {
                    return produceScript(traversalSource, (Bytecode) o);
                } else {
                    return convertToScript(o);
                }
            }

            /**
             * Gets the syntax for the spawn of an anonymous traversal which is traditionally the double underscore.
             */
            protected String getAnonymousTraversalPrefix() {
                return ANONYMOUS_TRAVERSAL_PREFIX;
            }

            /**
             * Gets the syntax for a {@code null} value as a string representation.
             */
            protected abstract String getNullSyntax();

            /**
             * Take the string argument and convert it to a string representation in the target language (i.e. escape,
             * enclose in appropriate quotes, etc.)
             */
            protected abstract String getSyntax(final String o);

            /**
             * Take the boolean argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final Boolean o);

            /**
             * Take the {@code Date} argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final Date o);

            /**
             * Take the {@code Timestamp} argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final Timestamp o);

            /**
             * Take the {@code UUID} argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final UUID o);

            /**
             * Take the {@link Lambda} argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final Lambda o);

            /**
             * Take the {@link SackFunctions.Barrier} argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final SackFunctions.Barrier o);

            /**
             * Take the {@link VertexProperty.Cardinality} argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final VertexProperty.Cardinality o);

            /**
             * Take the {@link TraversalOptionParent.Pick} argument and convert it to a string representation in the target language.
             */
            protected abstract String getSyntax(final TraversalOptionParent.Pick o);

            /**
             * Take the numeric argument and convert it to a string representation in the target language. Languages
             * that can discern differences in types of numbers will wish to further check the type of the
             * {@code Number} instance itself (i.e. {@code Double}, {@code Integer}, etc.)
             */
            protected abstract String getSyntax(final Number o);

            /**
             * Take the {@code Set} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final Set<?> o);

            /**
             * Take the {@code List} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final List<?> o);

            /**
             * Take the {@code Map} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final Map<?,?> o);

            /**
             * Take the {@code Class} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final Class<?> o);

            /**
             * Take the {@code Enum} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final Enum<?> o);

            /**
             * Take the {@link Vertex} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final Vertex o);

            /**
             * Take the {@link Edge} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final Edge o);

            /**
             * Take the {@link VertexProperty} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final VertexProperty<?> o);

            /**
             * Take the {@link TraversalStrategyProxy} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final TraversalStrategyProxy<?> o);

            /**
             * Take the {@link Bytecode} and writes the syntax directly to the member {@link #script} variable.
             */
            protected abstract Script produceScript(final String traversalSource, final Bytecode o);

            /**
             * Take the {@link P} and writes the syntax directly to the member {@link #script} variable. This
             * implementation should also consider {@link TextP}.
             */
            protected abstract Script produceScript(final P<?> p);

            /**
             *  For each operator argument, if withParameters set true, try parametrization as follows:
             *
             *  -----------------------------------------------
             *  if unpack, why ?      ObjectType
             *  -----------------------------------------------
             *  (Yes)                 Bytecode.Binding
             *  (Recursion, No)       Bytecode
             *  (Recursion, No)       Traversal
             *  (Yes)                 String
             *  (Recursion, No)       Set
             *  (Recursion, No)       List
             *  (Recursion, No)       Map
             *  (Yes)                 Long
             *  (Yes)                 Double
             *  (Yes)                 Float
             *  (Yes)                 Integer
             *  (Yes)                 Timestamp
             *  (Yes)                 Date
             *  (Yes)                 Uuid
             *  (Recursion, No)       P
             *  (Enumeration, No)     SackFunctions.Barrier
             *  (Enumeration, No)     VertexProperty.Cardinality
             *  (Enumeration, No)     TraversalOptionParent.Pick
             *  (Enumeration, No)     Enum
             *  (Recursion, No)       Vertex
             *  (Recursion, No)       Edge
             *  (Recursion, No)       VertexProperty
             *  (Yes)                 Lambda
             *  (Recursion, No)       TraversalStrategyProxy
             *  (Enumeration, No)     TraversalStrategy
             *  (Yes)                 Other
             *  -------------------------------------------------
             *
             * @param object
             * @return String Repres
             */
            protected Script convertToScript(final Object object) {
                if (object instanceof Bytecode.Binding) {
                    return script.getBoundKeyOrAssign(withParameters, ((Bytecode.Binding) object).variable());
                } else if (object instanceof Bytecode) {
                    return produceScript(getAnonymousTraversalPrefix(), (Bytecode) object);
                } else if (object instanceof Traversal) {
                    return convertToScript(((Traversal) object).asAdmin().getBytecode());
                } else if (object instanceof String) {
                    final Object objectOrWrapper = withParameters ? object : getSyntax((String) object);
                    return script.getBoundKeyOrAssign(withParameters, objectOrWrapper);
                } else if (object instanceof Boolean) {
                    final Object objectOrWrapper = withParameters ? object : getSyntax((Boolean) object);
                    return script.getBoundKeyOrAssign(withParameters, objectOrWrapper);
                } else if (object instanceof Set) {
                    return produceScript((Set<?>) object);
                } else if (object instanceof List) {
                    return produceScript((List<?>) object);
                } else if (object instanceof Map) {
                    return produceScript((Map<?, ?>) object);
                } else if (object instanceof Number) {
                    final Object objectOrWrapper = withParameters ? object : getSyntax((Number) object);
                    return script.getBoundKeyOrAssign(withParameters, objectOrWrapper);
                } else if (object instanceof Class) {
                    return produceScript((Class<?>) object);
                } else if (object instanceof Timestamp) {
                    final Object objectOrWrapper = withParameters ? object : getSyntax((Timestamp) object);
                    return script.getBoundKeyOrAssign(withParameters, objectOrWrapper);
                } else if (object instanceof Date) {
                    final Object objectOrWrapper = withParameters ? object : getSyntax((Date) object);
                    return script.getBoundKeyOrAssign(withParameters, objectOrWrapper);
                } else if (object instanceof UUID) {
                    final Object objectOrWrapper = withParameters ? object : getSyntax((UUID) object);
                    return script.getBoundKeyOrAssign(withParameters, objectOrWrapper);
                } else if (object instanceof P) {
                    return produceScript((P<?>) object);
                } else if (object instanceof SackFunctions.Barrier) {
                    return script.append(getSyntax((SackFunctions.Barrier) object));
                } else if (object instanceof VertexProperty.Cardinality) {
                    return script.append(getSyntax((VertexProperty.Cardinality) object));
                } else if (object instanceof TraversalOptionParent.Pick) {
                    return script.append(getSyntax((TraversalOptionParent.Pick) object));
                } else if (object instanceof Enum) {
                    return produceScript((Enum<?>) object);
                } else if (object instanceof Vertex) {
                    return produceScript((Vertex) object);
                } else if (object instanceof Edge) {
                    return produceScript((Edge) object);
                } else if (object instanceof VertexProperty) {
                    return produceScript((VertexProperty<?>) object);
                } else if (object instanceof Lambda) {
                    final Object objectOrWrapper = withParameters ? object : getSyntax((Lambda) object);
                    return script.getBoundKeyOrAssign(withParameters,objectOrWrapper);
                } else if (object instanceof TraversalStrategyProxy) {
                    return produceScript((TraversalStrategyProxy<?>) object);
                } else if (object instanceof TraversalStrategy) {
                    return convertToScript(new TraversalStrategyProxy(((TraversalStrategy) object)));
                } else {
                    return null == object ? script.append(getNullSyntax()) : script.getBoundKeyOrAssign(withParameters, object);
                }
            }
        }
    }

    /**
     * Translates bytecode to actual steps.
     */
    public interface StepTranslator<S extends TraversalSource, T extends Traversal.Admin<?, ?>> extends Translator<S, T> {

    }
}
