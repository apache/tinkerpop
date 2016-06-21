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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Translator extends Cloneable {

    public String getAlias();

    public void addStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments);

    public default void addSpawnStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        addStep(traversal, stepName, arguments);
    }

    public default void addSource(final TraversalSource traversalSource, final String sourceName, final Object... arguments) {
        addStep(EmptyTraversal.instance(), sourceName, arguments);
    }

    public Translator getAnonymousTraversalTranslator();

    public String getTraversalScript();

    public Translator clone();

    public String getSourceLanguage();

    public String getTargetLanguage();
}
