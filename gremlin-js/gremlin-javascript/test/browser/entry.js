/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * Bundle entry point for the browser smoke tests. Imports the package the same way a consumer
 * would (its public entry points, resolved through the "browser" field), then exposes the result
 * on `window` so the Playwright specs can drive it via `page.evaluate`.
 */
import * as gremlin from '../../build/esm/index.js';
import * as gremlinLanguage from '../../build/esm/language/index.js';

globalThis.__gremlinBrowserSmoke = { gremlin, gremlinLanguage };
