/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import { ProviderDefinedType } from './graph.js';

export interface PdtAdapter {
  serialize: (obj: any) => Record<string, any>;
  deserialize: (fields: Record<string, any>) => any;
}

/**
 * A standalone registry that allows users to register adapters for hydrating
 * raw {@link ProviderDefinedType} instances into domain-specific objects.
 */
export class ProviderDefinedTypeRegistry {
  private readonly _adapters: Map<string, PdtAdapter> = new Map();
  private readonly _adaptersByClass: Map<Function, { typeName: string; adapter: PdtAdapter }> = new Map();

  register(typeName: string, adapter: PdtAdapter, targetClass?: Function): void {
    this._adapters.set(typeName, adapter);
    if (targetClass) {
      this._adaptersByClass.set(targetClass, { typeName, adapter });
    }
  }

  hydrate(pdt: any): any {
    if (!(pdt instanceof ProviderDefinedType)) return pdt;
    const adapter = this._adapters.get(pdt.name);
    const hydratedFields: Record<string, any> = {};
    let changed = false;
    for (const [k, v] of Object.entries(pdt.fields)) {
      if (v instanceof ProviderDefinedType) {
        const h = this.hydrate(v);
        hydratedFields[k] = h;
        if (h !== v) changed = true;
      } else {
        hydratedFields[k] = v;
      }
    }
    if (!adapter) {
      return changed ? new ProviderDefinedType(pdt.name, hydratedFields) : pdt;
    }
    try {
      return adapter.deserialize(hydratedFields);
    } catch (e: any) {
      console.warn(`PDT hydration failed for '${pdt.name}': ${e.message}`);
      return pdt;
    }
  }

  hasAdapter(typeName: string): boolean {
    return this._adapters.has(typeName);
  }

  getSerializer(typeName: string): ((obj: any) => Record<string, any>) | null {
    const adapter = this._adapters.get(typeName);
    return adapter ? adapter.serialize : null;
  }

  getAdapterByClass(cls: Function): { typeName: string; serialize: (obj: any) => Record<string, any> } | null {
    const entry = this._adaptersByClass.get(cls);
    if (!entry) return null;
    return { typeName: entry.typeName, serialize: entry.adapter.serialize };
  }
}
