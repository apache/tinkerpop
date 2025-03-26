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
package org.apache.tinkerpop.gremlin.language.translator;

import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;

/**
 * A base class for visitors that translate Gremlin AST nodes to Gremlin strings.
 */
public abstract class AbstractTranslateVisitor extends TranslateVisitor {

    public AbstractTranslateVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    public Void visitStringLiteral(final GremlinParser.StringLiteralContext ctx) {
        // remove the first and last character (single or double quotes)
        final String text = removeFirstAndLastCharacters(ctx.getText());
        handleStringLiteralText(text);
        return null;
    }

    @Override
    public Void visitStringNullableLiteral(final GremlinParser.StringNullableLiteralContext ctx) {
        // remove the first and last character (single or double quotes) but only if it is not null
        if (ctx.getText().equals("null")) {
            sb.append("null");
        } else {
            final String text = removeFirstAndLastCharacters(ctx.getText());
            handleStringLiteralText(text);
        }
        return null;
    }

    protected void handleStringLiteralText(final String text) {
        sb.append("\"");
        sb.append(text);
        sb.append("\"");
    }

    @Override
    public Void visitKeyword(final GremlinParser.KeywordContext ctx) {
        final String keyword = ctx.getText();

        // translate differently based on the context of the keyword's parent.
        if (ctx.getParent() instanceof GremlinParser.MapEntryContext) {
            // if the keyword is a key in a map, then it's a string literal essentially
            handleStringLiteralText(keyword);
        } else {
            // in all other cases it's used more like "new Class()"
            sb.append(keyword).append(" ");
        }

        return null;
    }
}
