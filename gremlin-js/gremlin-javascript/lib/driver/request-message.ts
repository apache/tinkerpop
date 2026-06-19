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

import GremlinLang from '../process/gremlin-lang.js';

// Token constants
const Tokens = {
  ARGS_LANGUAGE: 'language',
  ARGS_BINDINGS: 'bindings',
  ARGS_G: 'g',
  ARGS_MATERIALIZE_PROPERTIES: 'materializeProperties',
  TIMEOUT_MS: 'timeoutMs',
  BULK_RESULTS: 'bulkResults',
  BATCH_SIZE: 'batchSize',
  MATERIALIZE_PROPERTIES_TOKENS: 'tokens',
  MATERIALIZE_PROPERTIES_ALL: 'all'
};

/**
 * The model for a request message in the HTTP body that is sent to the server beginning in 4.0.0.
 */
export class RequestMessage {
  private gremlin: string;
  private language: string;
  private timeoutMs?: number;
  private bindings?: object;
  private g?: string;
  private materializeProperties?: string;
  private bulkResults?: boolean;
  private batchSize?: number;
  private customFields: Map<string, any>;

  private constructor(
    gremlin: string,
    language: string,
    timeoutMs: number | undefined,
    bindings: object | undefined,
    g: string | undefined,
    materializeProperties: string | undefined,
    bulkResults: boolean | undefined,
    batchSize: number | undefined,
    customFields: Map<string, any>
  ) {
    if (!gremlin) {
      throw new Error('RequestMessage requires gremlin argument');
    }

    this.gremlin = gremlin;
    this.language = language;
    this.timeoutMs = timeoutMs;
    this.bindings = bindings;
    this.g = g;
    this.materializeProperties = materializeProperties;
    this.bulkResults = bulkResults;
    this.batchSize = batchSize;
    this.customFields = customFields;
  }

  getGremlin(): string {
    return this.gremlin;
  }

  getLanguage(): string {
    return this.language;
  }

  getTimeoutMs(): number | undefined {
    return this.timeoutMs;
  }

  getBindings(): object | undefined {
    return this.bindings;
  }

  getG(): string | undefined {
    return this.g;
  }

  getMaterializeProperties(): string | undefined {
    return this.materializeProperties;
  }

  getBulkResults(): boolean | undefined {
    return this.bulkResults;
  }

  getBatchSize(): number | undefined {
    return this.batchSize;
  }

  getFields(): ReadonlyMap<string, any> {
    return this.customFields;
  }

  /**
   * Builds the plain object that represents this message on the wire. Standard fields are
   * included when set, and custom fields are flattened to the top level. This method is
   * invoked automatically by {@link JSON.stringify}.
   *
   * When a new standard field is added to this class, it should be added here as well so that
   * it is included in the serialized request body.
   */
  toJSON(): Record<string, any> {
    const payload: Record<string, any> = { gremlin: this.gremlin };

    if (this.language) {
      payload['language'] = this.language;
    }
    if (this.g) {
      payload['g'] = this.g;
    }
    if (this.bindings !== undefined) {
      payload['bindings'] = this.bindings;
    }
    if (this.timeoutMs !== undefined) {
      payload['timeoutMs'] = this.timeoutMs;
    }
    if (this.materializeProperties) {
      payload['materializeProperties'] = this.materializeProperties;
    }
    if (this.bulkResults !== undefined) {
      payload['bulkResults'] = this.bulkResults;
    }
    if (this.batchSize !== undefined) {
      payload['batchSize'] = this.batchSize;
    }

    // Flatten custom/provider fields to the top level
    this.customFields.forEach((v, k) => {
      payload[k] = v;
    });

    return payload;
  }

  static build(gremlin: string): Builder {
    return new Builder(gremlin);
  }
}

/**
 * Builder class for RequestMessage.
 */
export class Builder {
  private readonly gremlin: string;
  private readonly bindings = {};
  private bindingsString?: string;
  public language: string;
  public timeoutMs?: number;
  public g?: string;
  public materializeProperties?: string;
  public bulkResults?: boolean;
  public batchSize?: number;
  public additionalFields = new Map<string, any>();

  constructor(gremlin: string) {
    this.gremlin = gremlin;
    this.language = "gremlin-lang";
  }

  addLanguage(language: string): Builder {
    if (!language) throw new Error('language argument cannot be null.');
    this.language = language;
    return this;
  }

  /**
   * Adds a single binding parameter. The accumulated bindings map is converted to a gremlin-lang
   * map literal string when the message is created.
   * Cannot be mixed with {@link addBindingsString}.
   */
  addBinding(key: string, value: any): Builder {
    if (this.bindingsString) throw new Error('Cannot mix addBinding() with addBindingsString().');
    Object.assign(this.bindings, {[key]: value})
    return this;
  }

  /**
   * Merges the provided bindings into the accumulated bindings map. The values are converted to a
   * gremlin-lang map literal string when the message is created.
   * Cannot be mixed with {@link addBindingsString}.
   */
  addBindings(otherBindings: object): Builder {
    if (!otherBindings) throw new Error('bindings argument cannot be null.');
    if (this.bindingsString) throw new Error('Cannot mix addBindings() with addBindingsString().');
    Object.assign(this.bindings, otherBindings)
    return this;
  }

  /**
   * Sets the bindings as a pre-formatted gremlin-lang map literal string (e.g. `'["x":1,"y":"knows"]'`).
   * Calling this method replaces any previously set bindings string (last-one-wins).
   * Cannot be mixed with {@link addBinding} or {@link addBindings}.
   */
  addBindingsString(bindingsString: string): Builder {
    if (Object.keys(this.bindings).length > 0) throw new Error('Cannot mix addBindingsString() with addBinding()/addBindings().');
    this.bindingsString = bindingsString;
    return this;
  }

  addG(g: string): Builder {
    if (!g) throw new Error('g argument cannot be null.');
    this.g = g;
    return this;
  }

  addMaterializeProperties(materializeProps: string): Builder {
    if (!materializeProps) throw new Error('materializeProps argument cannot be null.');
    if (materializeProps !== Tokens.MATERIALIZE_PROPERTIES_TOKENS && 
        materializeProps !== Tokens.MATERIALIZE_PROPERTIES_ALL) {
      throw new Error(`materializeProperties argument must be either "${Tokens.MATERIALIZE_PROPERTIES_TOKENS}" or "${Tokens.MATERIALIZE_PROPERTIES_ALL}".`);
    }

    this.materializeProperties = materializeProps;
    return this;
  }

  addTimeoutMillis(timeout: number): Builder {
    if (timeout < 0) throw new Error('timeout argument cannot be negative.');
    this.timeoutMs = timeout;
    return this;
  }

  addBulkResults(bulking: boolean): Builder {
    this.bulkResults = bulking;
    return this;
  }

  addBatchSize(batchSize: number): Builder {
    if (batchSize <= 0) throw new Error('batchSize argument must be positive.');
    this.batchSize = batchSize;
    return this;
  }

  addField(key: string, value: any): Builder {
    this.additionalFields.set(key, value);
    return this;
  }

  /**
   * Create the request message given the settings provided to the Builder.
   */
  create(): RequestMessage {
    // mutual exclusion between addBindings/addBinding and addBindingsString is
    // enforced at call time, so only one path can be set here
    let bindings: string | undefined;
    if (this.bindingsString) {
      bindings = this.bindingsString;
    } else if (Object.keys(this.bindings).length > 0) {
      bindings = GremlinLang.convertParametersToString(new Map(Object.entries(this.bindings)));
    }
    // @ts-ignore - accessing private constructor from Builder
    return new RequestMessage(
      this.gremlin,
      this.language || 'gremlin-lang',
      this.timeoutMs,
      bindings,
      this.g,
      this.materializeProperties,
      this.bulkResults,
      this.batchSize,
      this.additionalFields
    );
  }
}
