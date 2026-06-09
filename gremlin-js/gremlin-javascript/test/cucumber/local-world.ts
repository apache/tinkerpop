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
 * Cucumber world for Tiny Gremlin (@TinyGremlin) local execution tests.
 * Replaces the remote-connection world.js with local Graph instances.
 * No Gremlin Server required.
 */

import { setWorldConstructor, Before } from '@cucumber/cucumber';
import { buildModernGraph, buildSinkGraph, buildCrewGraph, buildEmptyGraph } from './local-fixtures.js';

// Per-scenario graph fixtures, keyed by graph name.
// Rebuilt fresh for every scenario to prevent cross-scenario mutation.
const graphFixtures: Record<string, any> = {};

function withConn(fixture: any): any {
  fixture.connection = fixture.graph;
  return fixture;
}

function resetFixtures() {
  graphFixtures['modern'] = withConn(buildModernGraph());
  graphFixtures['sink']   = withConn(buildSinkGraph());
  graphFixtures['crew']   = withConn(buildCrewGraph());
  graphFixtures['empty']  = withConn(buildEmptyGraph());
}

class TinkerPopLocalWorld {
  scenario: string | null = null;
  g: any = null;
  traversal: any = null;
  result: any = null;
  cache: Record<string, any> | null = null;
  graphName: string | null = null;
  parameters: Record<string, any> = {};
  sideEffects: Record<string, any> = {};
  isGraphComputer = false;

  getData() {
    if (!this.graphName) throw new Error('Graph name is not set');
    return graphFixtures[this.graphName];
  }

  cleanEmptyGraph() {
    graphFixtures['empty'] = withConn(buildEmptyGraph());
    return Promise.resolve();
  }

  loadEmptyGraphData() {
    return Promise.resolve();
  }
}

setWorldConstructor(TinkerPopLocalWorld);

// Rebuild all graph fixtures before every scenario to prevent cross-scenario mutation.
Before(function (this: TinkerPopLocalWorld, info: any) {
  this.scenario = info.pickle.name;
  resetFixtures();
  this.cache = graphFixtures;
});

Before({ tags: '@GraphComputerOnly' },       () => 'skipped');
Before({ tags: '@AllowNullPropertyValues' },  () => 'skipped');
Before({ tags: '@StepWrite' },               () => 'skipped');
Before({ tags: '@DataChar' },                () => 'skipped');
Before({ tags: '@DataDuration' },            () => 'skipped');
