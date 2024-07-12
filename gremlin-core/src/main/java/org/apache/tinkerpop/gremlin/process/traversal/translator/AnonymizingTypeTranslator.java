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

package org.apache.tinkerpop.gremlin.process.traversal.translator;


import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Script;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This Translator will translate {@link Bytecode} into a representation that has been stripped of any user data
 * (anonymized). A default anonymizer is provided, but can be replaced with a custom anonymizer as needed. The
 * default anonymizer replaces any String, Numeric, Date, Timestamp, or UUID with a type-based token. Identical values
 * will receive the same token (e.g. if "foo" is assigned "string0" then all occurrences of "foo" will be replaced
 * with "string0").
 */
public class AnonymizingTypeTranslator extends GroovyTranslator.DefaultTypeTranslator {

    /**
     * Customizable anonymizer interface.
     */
    public interface Anonymizer {
        /**
         * Return an anonymized token for the supplied object.
         *
         * @param obj a {@link Traversal} object of one of the following types: String, Long, Double, FLoat, Integer,
         *            Class, TImestamp, Date, UUID, {@link Vertex}, {@link Edge}, {@link VertexProperty}
         * @return    an anonymized version of the supplied object
         */
        Object anonymize(Object obj);
    }

    /**
     * This default implementation keeps a map from type (Java Class) to another map from instances to anonymized
     * token.
     */
    public static class DefaultAnonymizer implements Anonymizer {
        /*
         * Map<ClassName, Map<Object, AnonymizedValue>>
         */
        private final Map<String, Map<Object, String>> simpleNameToObjectCache = new HashMap<>();

        /**
         * Return an anonymized token for the supplied object of the form "type:instance#".
         */
        @Override
        public Object anonymize(final Object obj) {
            final String type = obj.getClass().getSimpleName();

            Map<Object, String> objectToAnonymizedString = simpleNameToObjectCache.get(type);
            if (objectToAnonymizedString != null){
                // this object type has been handled at least once before
                final String innerValue = objectToAnonymizedString.get(obj);
                if (innerValue != null){
                    return innerValue;
                } else {
                    final String anonymizedValue = type.toLowerCase() + objectToAnonymizedString.size();
                    objectToAnonymizedString.put(obj, anonymizedValue);
                    return anonymizedValue;
                }
            } else {
                objectToAnonymizedString = new HashMap<>();
                simpleNameToObjectCache.put(type, objectToAnonymizedString);
                final String anonymizedValue = type.toLowerCase() + objectToAnonymizedString.size();
                objectToAnonymizedString.put(obj, anonymizedValue);
                return anonymizedValue;
            }
        }
    }

    private final Anonymizer anonymizer;

    /**
     * Default constructor creates a {@link DefaultAnonymizer} + withParameters=false.
     */
    public AnonymizingTypeTranslator() {
        this(new DefaultAnonymizer(), false);
    }

    public AnonymizingTypeTranslator(final boolean withParameters) {
        this(new DefaultAnonymizer(), withParameters);
    }

    public AnonymizingTypeTranslator(final Anonymizer anonymizer, final boolean withParameters) {
        super(withParameters);
        this.anonymizer = anonymizer;
    }

    @Override
    protected String getSyntax(final String o) {
        return anonymizer.anonymize(o).toString();
    }

    @Override
    protected String getSyntax(final Date o) {
        return anonymizer.anonymize(o).toString();
    }

    @Override
    protected String getSyntax(final OffsetDateTime o) {
        return anonymizer.anonymize(o).toString();
    }

    @Override
    protected String getSyntax(final Timestamp o) {
        return anonymizer.anonymize(o).toString();
    }

    @Override
    protected String getSyntax(final UUID o) {
        return anonymizer.anonymize(o).toString();
    }

    @Override
    protected String getSyntax(final Number o) {
        return anonymizer.anonymize(o).toString();
    }

    @Override
    protected Script produceScript(final Class<?> o) {
        return script.append(anonymizer.anonymize(o).toString());
    }

}
