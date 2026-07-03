/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Names that, when they show up as unresolved callees (external Function stubs),
 * are almost certainly JDK / standard-library / ubiquitous-accessor calls rather
 * than project functions. Classifying these as `library` lets structural checks
 * (centrality, blast radius) drop the noise they'd otherwise inflate.
 *
 * Deliberately conservative: it lists object/collection/string/IO/logging/stream
 * staples and common getX/isX accessors, and avoids ambiguous verbs (build,
 * create, start, run, call…) that are just as likely to name a real project
 * method. A false "library" on a genuinely ubiquitous name like `get` is
 * acceptable; a false "library" on a distinctive project method is not.
 */
export const COMMON_LIBRARY_NAMES = new Set([
  // java.lang.Object
  "equals", "hashCode", "toString", "clone", "finalize", "getClass",
  "wait", "notify", "notifyAll",
  // Comparable / iterator
  "compareTo", "iterator", "hasNext", "next", "remove",
  // Collection / Map / List
  "size", "isEmpty", "contains", "containsKey", "containsValue",
  "add", "addAll", "get", "set", "put", "putAll", "clear", "remove",
  "keySet", "values", "entrySet", "toArray", "stream", "getKey", "getValue",
  // String / CharSequence
  "length", "charAt", "substring", "trim", "split", "replace", "indexOf",
  "startsWith", "endsWith", "matches", "toLowerCase", "toUpperCase",
  "getBytes", "valueOf", "format",
  // common accessors
  "getName", "getId", "getType", "getMessage", "getValue", "getStatusCode",
  "getCause", "getClassName", "name", "ordinal",
  // IO / lifecycle
  "close", "flush", "read", "write", "open",
  // logging
  "log", "info", "debug", "warn", "error", "trace", "printStackTrace",
  "print", "println",
  // Optional / functional / stream
  "of", "empty", "orElse", "orElseGet", "orElseThrow", "ifPresent", "isPresent",
  "forEach", "map", "filter", "collect", "count", "findFirst", "anyMatch",
  "allMatch", "noneMatch", "apply", "accept", "test",
]);

/**
 * @param {string} name
 * @returns {boolean} true if the name is a well-known library/JDK/accessor call
 */
export function isLibraryName(name) {
  return COMMON_LIBRARY_NAMES.has(name);
}

/**
 * Origin classification for an external stub, given whether a project type with
 * this name was found in the repo.
 *
 *   library    — a known JDK/standard-library/accessor name (noise)
 *   project    — a project source file declares a type with this name (signal)
 *   unresolved — neither; unknown, treat with caution
 *
 * Method-name project detection isn't attempted (name-only grep can't tell a
 * definition from a use reliably); that's the domain of precise resolution.
 *
 * @param {string} name
 * @param {boolean} definedAsProjectType
 * @returns {"library"|"project"|"unresolved"}
 */
export function classifyExternalName(name, definedAsProjectType) {
  if (isLibraryName(name)) return "library";
  if (definedAsProjectType) return "project";
  return "unresolved";
}
