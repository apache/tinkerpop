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

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * Antlr parse tree context cast helper.
 */
public class ParseTreeContextCastHelper {

    /**
     * Cast ParseTree node child into GenericLiteralContext
     * @param ctx : ParseTree node
     * @param childIndex : child index
     * @return casted GenericLiteralContext
     */
    public static GremlinParser.GenericLiteralContext castChildToGenericLiteral(final ParseTree ctx, final int childIndex) {
        return (GremlinParser.GenericLiteralContext)(ctx.getChild(childIndex));
    }

    /**
     * Cast ParseTree node child into GenericLiteralListContext
     * @param ctx : ParseTree node
     * @param childIndex : child index
     * @return casted GenericLiteralListContext
     */
    public static GremlinParser.GenericLiteralListContext castChildToGenericLiteralList(final ParseTree ctx, final int childIndex) {
        return (GremlinParser.GenericLiteralListContext)(ctx.getChild(childIndex));
    }
}
