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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

public class StructureElementVisitor extends GremlinBaseVisitor<Element> {

    private StructureElementVisitor() {}

    private static StructureElementVisitor instance;

    public static StructureElementVisitor instance() {
        if (instance == null) {
            instance = new StructureElementVisitor();
        }
        return instance;
    }

    @Override
    public Vertex visitStructureVertex(final GremlinParser.StructureVertexContext ctx) {
        return new ReferenceVertex(GenericLiteralVisitor.instance().visitGenericLiteral(ctx.genericLiteral()),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }
}
