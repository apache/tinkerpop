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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * Utility class for parsing {@link Bytecode}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.4.9, replaced by {@link BytecodeHelper}.
 */
public final class BytecodeUtil {

    private BytecodeUtil() {}

    /**
     * Parses {@link Bytecode} to find {@link TraversalStrategy} objects in the source instructions.
     * @deprecated As of release 3.4.9, replaced by {@link BytecodeHelper#findStrategies(Bytecode, Class)}.
     */
    @Deprecated
    public static <A extends TraversalStrategy> Iterator<A> findStrategies(final Bytecode bytecode, final Class<A> clazz) {
        return IteratorUtils.map(
                IteratorUtils.filter(bytecode.getSourceInstructions().iterator(),
                        s -> s.getOperator().equals(TraversalSource.Symbols.withStrategies) && clazz.isAssignableFrom(s.getArguments()[0].getClass())),
                os -> (A) os.getArguments()[0]);
    }
}
