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

import {
  ATNSimulator, BaseErrorListener, CharStream, CommonTokenStream,
  PredictionMode, Recognizer, RecognitionException,
} from 'antlr4ng';
import { GremlinLexer } from '../language/grammar/GremlinLexer.js';
import { GremlinParser } from '../language/grammar/GremlinParser.js';
import { ExecuteVisitor } from '../language/executor/ExecuteVisitor.js';
import { LocalExecutor } from '../process/local/LocalExecutor.js';
import { RemoteConnection, RemoteTraversal } from './remote-connection.js';
import { Graph } from '../structure/graph.js';
import type GremlinLang from '../process/gremlin-lang.js';

class ThrowingErrorListener extends BaseErrorListener {
  override syntaxError(
    _recognizer: Recognizer<ATNSimulator>,
    _offendingSymbol: unknown,
    line: number,
    column: number,
    msg: string,
    _e: RecognitionException | null,
  ): void {
    throw new Error(`Tiny Gremlin parse error at ${line}:${column} — ${msg}`);
  }
}

const errorListener = new ThrowingErrorListener();

/**
 * A RemoteConnection implementation that executes traversals locally against an
 * in-memory Graph using the Tiny Gremlin subset. No network is involved.
 *
 * Usage:
 *   const g = anon.traversal().with_(myGraph);
 *   // or equivalently:
 *   const g = anon.traversal().with_(new LocalGraphConnection(myGraph));
 */
export class LocalGraphConnection extends RemoteConnection {
  private readonly graph: Graph;
  private readonly executor: LocalExecutor;

  constructor(graph: Graph) {
    super('local://');
    this.graph = graph;
    this.executor = new LocalExecutor();
  }

  override async open(): Promise<void> {
    // no-op: no connection to open
  }

  override get isOpen(): boolean {
    return true;
  }

  override async submit(gremlinLang: GremlinLang): Promise<RemoteTraversal> {
    const query = gremlinLang.getGremlin();
    const pipeline = this.parse(query);
    const generator = this.executor.execute(pipeline, this.graph);
    return new RemoteTraversal(generator);
  }

  override async commit(): Promise<void> {
    // no-op: local graph mutations are in-memory only
  }

  override async rollback(): Promise<void> {
    // no-op
  }

  override async close(): Promise<void> {
    // no-op
  }

  private parse(query: string) {
    const chars = CharStream.fromString(query);
    const lexer = new GremlinLexer(chars);
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);

    const tokens = new CommonTokenStream(lexer);
    const parser = new GremlinParser(tokens);
    parser.interpreter.predictionMode = PredictionMode.SLL;
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);

    let queryCtx: ReturnType<typeof parser.queryList>;
    try {
      queryCtx = parser.queryList();
    } catch {
      tokens.seek(0);
      lexer.reset();
      parser.reset();
      parser.interpreter.predictionMode = PredictionMode.LL;
      queryCtx = parser.queryList();
    }

    const visitor = new ExecuteVisitor();
    visitor.visit(queryCtx);
    return visitor.getPipeline();
  }
}
