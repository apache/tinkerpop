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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTranslator implements Translator<Traversal.Admin<?, ?>, TraversalSource> {

    private static final EmptyTranslator INSTANCE = new EmptyTranslator();

    private EmptyTranslator() {
        // instance only
    }

    @Override
    public String getAlias() {
        return "";
    }

    @Override
    public Traversal.Admin<?, ?> addStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        return traversal;
    }

    @Override
    public Traversal.Admin<?, ?> addSpawnStep(TraversalSource traversalSource, String stepName, Object... arguments) {
        return EmptyTraversal.instance();
    }

    @Override
    public TraversalSource addSource(TraversalSource traversalSource, String sourceName, Object... arguments) {
        return traversalSource;
    }

    @Override
    public Translator getAnonymousTraversalTranslator() {
        return EmptyTranslator.INSTANCE;
    }

    @Override
    public String getTraversalScript() {
        return "";
    }

    @Override
    public Translator clone() {
        return this;
    }

    @Override
    public String getSourceLanguage() {
        return "none";
    }

    @Override
    public String getTargetLanguage() {
        return "none";
    }

    public static final EmptyTranslator instance() {
        return INSTANCE;
    }
}
