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

// Token constants
const Tokens = {
  ARGS_LANGUAGE: 'language',
  ARGS_BINDINGS: 'bindings',
  ARGS_G: 'g',
  ARGS_MATERIALIZE_PROPERTIES: 'materializeProperties',
  TIMEOUT_MS: 'timeoutMs',
  BULK_RESULTS: 'bulkResults',
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
  private customFields: Map<string, any>;

  private constructor(
    gremlin: string,
    language: string,
    timeoutMs: number | undefined,
    bindings: object | undefined,
    g: string | undefined,
    materializeProperties: string | undefined,
    bulkResults: boolean | undefined,
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

  getFields(): ReadonlyMap<string, any> {
    return this.customFields;
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
  public language?: string;
  public timeoutMs?: number;
  public g?: string;
  public materializeProperties?: string;
  public bulkResults?: boolean;
  public additionalFields = new Map<string, any>();

  constructor(gremlin: string) {
    this.gremlin = gremlin;
  }

  addLanguage(language: string): Builder {
    if (!language) throw new Error('language argument cannot be null.');
    this.language = language;
    return this;
  }

  addBinding(key: string, value: any): Builder {
    Object.assign(this.bindings, {key: value})
    return this;
  }

  addBindings(otherBindings: object): Builder {
    if (!otherBindings) throw new Error('bindings argument cannot be null.');
    Object.assign(this.bindings, otherBindings)
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

  addField(key: string, value: any): Builder {
    this.additionalFields.set(key, value);
    return this;
  }

  /**
   * Create the request message given the settings provided to the Builder.
   */
  create(): RequestMessage {
    // @ts-ignore - accessing private constructor from Builder
    return new RequestMessage(
      this.gremlin,
      this.language || 'gremlin-lang',
      this.timeoutMs,
      Object.keys(this.bindings).length > 0 ? this.bindings : undefined,
      this.g,
      this.materializeProperties,
      this.bulkResults,
      this.additionalFields
    );
  }
}
