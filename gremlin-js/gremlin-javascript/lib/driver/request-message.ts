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
  ARGS_PARAMETERS: 'parameters',
  ARGS_G: 'g',
  ARGS_MATERIALIZE_PROPERTIES: 'materializeProperties',
  TIMEOUT_MILLIS: 'timeoutMillis',
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
  private timeoutMillis?: number;
  private parameters?: object;
  private g?: string;
  private materializeProperties?: string;
  private bulkResults?: boolean;
  private batchSize?: number;
  private customFields: Map<string, any>;

  private constructor(
    gremlin: string,
    language: string,
    timeoutMillis: number | undefined,
    parameters: object | undefined,
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
    this.timeoutMillis = timeoutMillis;
    this.parameters = parameters;
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

  getTimeoutMillis(): number | undefined {
    return this.timeoutMillis;
  }

  getParameters(): object | undefined {
    return this.parameters;
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
    if (this.parameters !== undefined) {
      payload['parameters'] = this.parameters;
    }
    if (this.timeoutMillis !== undefined) {
      payload['timeoutMillis'] = this.timeoutMillis;
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
  private readonly parameters = {};
  private parametersString?: string;
  public language: string;
  public timeoutMillis?: number;
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
   * Adds a single query parameter. The accumulated parameters map is converted to a gremlin-lang
   * map literal string when the message is created.
   * Cannot be mixed with {@link addParametersString}.
   */
  addParameter(key: string, value: any): Builder {
    if (this.parametersString) throw new Error('Cannot mix addParameter() with addParametersString().');
    Object.assign(this.parameters, {[key]: value})
    return this;
  }

  /**
   * Merges the provided parameters into the accumulated parameters map. The values are converted to a
   * gremlin-lang map literal string when the message is created.
   * Cannot be mixed with {@link addParametersString}.
   */
  addParameters(otherParameters: object): Builder {
    if (!otherParameters) throw new Error('parameters argument cannot be null.');
    if (this.parametersString) throw new Error('Cannot mix addParameters() with addParametersString().');
    Object.assign(this.parameters, otherParameters)
    return this;
  }

  /**
   * Sets the query parameters as a pre-formatted gremlin-lang map literal string (e.g. `'["x":1,"y":"knows"]'`).
   * Calling this method replaces any previously set parameters string (last-one-wins).
   * Cannot be mixed with {@link addParameter} or {@link addParameters}.
   */
  addParametersString(parametersString: string): Builder {
    if (Object.keys(this.parameters).length > 0) throw new Error('Cannot mix addParametersString() with addParameter()/addParameters().');
    this.parametersString = parametersString;
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
    this.timeoutMillis = timeout;
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
    // mutual exclusion between addParameters/addParameter and addParametersString is
    // enforced at call time, so only one path can be set here
    let parameters: string | undefined;
    if (this.parametersString) {
      parameters = this.parametersString;
    } else if (Object.keys(this.parameters).length > 0) {
      parameters = GremlinLang.convertParametersToString(new Map(Object.entries(this.parameters)));
    }
    // @ts-ignore - accessing private constructor from Builder
    return new RequestMessage(
      this.gremlin,
      this.language || 'gremlin-lang',
      this.timeoutMillis,
      parameters,
      this.g,
      this.materializeProperties,
      this.bulkResults,
      this.batchSize,
      this.additionalFields
    );
  }
}
