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

import { ATNSimulator, BaseErrorListener, CharStream, CommonTokenStream, PredictionMode, Recognizer, RecognitionException } from 'antlr4ng';
import { GremlinLexer } from '../grammar/GremlinLexer.js';
import { GremlinParser } from '../grammar/GremlinParser.js';
import { Translation } from './Translation.js';
import { TranslatorException } from './TranslatorException.js';
import TranslateVisitor from './TranslateVisitor.js';
import JavascriptTranslateVisitor from './JavascriptTranslateVisitor.js';
import { Translator, TranslatorKey } from './Translator.js';

/**
 * Error listener that throws TranslatorException on syntax errors.
 */
class ThrowingErrorListener extends BaseErrorListener {
    override syntaxError(
        _recognizer: Recognizer<ATNSimulator>,
        _offendingSymbol: unknown,
        line: number,
        column: number,
        msg: string,
        _e: RecognitionException | null,
    ): void {
        throw new TranslatorException(`Failed to interpret Gremlin query at line ${line}:${column} - ${msg}`);
    }
}

const errorListener = new ThrowingErrorListener();

/**
 * Translates a Gremlin string into a source code representation using ANTLR.
 * Mirrors the Java GremlinTranslator with SLL → LL fallback parsing.
 */
export class GremlinTranslator {

    /**
     * Translates a Gremlin query string. Defaults to JavaScript output.
     *
     * @param query The Gremlin traversal string to translate.
     * @param graphTraversalSourceName Name of the traversal source variable (default: 'g').
     * @param translatorOrVisitor A TranslatorKey (e.g. 'JAVASCRIPT'), a custom
     *   TranslateVisitor instance, or omit for the default JavaScript translator.
     */
    static translate(
        query: string,
        graphTraversalSourceName?: string,
        translatorOrVisitor?: TranslatorKey | TranslateVisitor,
    ): Translation {
        const sourceName = graphTraversalSourceName ?? 'g';

        let visitor: TranslateVisitor;
        if (translatorOrVisitor == null) {
            visitor = new JavascriptTranslateVisitor(sourceName);
        } else if (typeof translatorOrVisitor === 'string') {
            visitor = Translator[translatorOrVisitor](sourceName);
        } else {
            visitor = translatorOrVisitor;
        }

        const chars = CharStream.fromString(query);
        const lexer = new GremlinLexer(chars);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        const tokens = new CommonTokenStream(lexer);
        const parser = new GremlinParser(tokens);
        // SLL mode is faster; fall back to LL on parse failure (mirrors Java impl)
        parser.interpreter.predictionMode = PredictionMode.SLL;
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        let queryContext: ReturnType<typeof parser.queryList>;
        try {
            queryContext = parser.queryList();
        } catch (_ex) {
            try {
                tokens.seek(0);
                lexer.reset();
                parser.reset();
                parser.interpreter.predictionMode = PredictionMode.LL;
                queryContext = parser.queryList();
            } catch (e) {
                throw new TranslatorException(
                    `Failed to interpret Gremlin query: ${e instanceof Error ? e.message : String(e)}`
                );
            }
        }

        visitor.visit(queryContext);

        return new Translation(query, visitor.getTranslated(), visitor.getParameters());
    }
}
