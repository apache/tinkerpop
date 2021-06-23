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

'use strict';

const assert = require('assert');
const { TraversalStrategies, OptionsStrategy, ConnectiveStrategy } = require('../../lib/process/traversal-strategy');

describe('TraversalStrategies', function () {

  describe('#removeStrategy()', function () {
    it('should remove strategy', function () {
      const ts = new TraversalStrategies()
      ts.addStrategy(new ConnectiveStrategy());
      ts.addStrategy(new OptionsStrategy({x: 123}));
      assert.strictEqual(ts.strategies.length, 2);

      const c = new OptionsStrategy({x: 123});
      const os = ts.removeStrategy(c);
      assert.strictEqual(os.fqcn, c.fqcn);
      assert.strictEqual(ts.strategies.length, 1);

      ts.removeStrategy(new ConnectiveStrategy());
      assert.strictEqual(ts.strategies.length, 0);
    });

    it('should not find anything to remove', function () {
      const ts = new TraversalStrategies()
      ts.addStrategy(new OptionsStrategy({x: 123}));
      assert.strictEqual(ts.strategies.length, 1);
      ts.removeStrategy(new ConnectiveStrategy());
      assert.strictEqual(ts.strategies.length, 1);
    });
  });
});