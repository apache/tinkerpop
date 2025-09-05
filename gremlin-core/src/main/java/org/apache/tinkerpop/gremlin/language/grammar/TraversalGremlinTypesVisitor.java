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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.apache.tinkerpop.gremlin.structure.GremlinDataType;

import java.util.Optional;

public class TraversalGremlinTypesVisitor {
    protected final GremlinAntlrToJava antlr;

    public TraversalGremlinTypesVisitor(final GremlinAntlrToJava antlrToJava) {
        this.antlr = antlrToJava;
    }

    public GremlinDataType visitGremlinDataType(final GremlinParser.TraversalGremlinTypesContext ctx) {
        return tryToConstructGremlinDataType(ctx.getText());
    }

    /**
     * Try to instantiate by checking registered {@link GremlinDataType} implementations that are
     * registered globally. Only strategies that are registered globally can be constructed in this way.
     */
    private static GremlinDataType tryToConstructGremlinDataType(final String typeName) {
        System.out.println(typeName);

        // try to grab the class from registered sources
        final Optional<GremlinDataType> opt = GremlinDataType.GlobalTypeCache.getRegisteredType(typeName);

        System.out.println(opt);

        if (opt.isEmpty())
            throw new IllegalStateException("GremlinDataType not recognized - " + typeName);

        return opt.get();

    }

}
