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

/**
 * @author Jorge Bay Gondra
 */
(function defineHelperModule() {
  "use strict";

  var assert = {
    ok: function (condition, message) {
      if (!condition) {
        throw new AssertError(message || (condition + ' == true'));
      }
    },
    strictEqual: function (val1, val2, message) {
      if (val1 !== val2) {
        throw new AssertError(message || (val1 + ' === ' + val2));
      }
    }
  };

  function loadLibModule(moduleName) {
    if (typeof require !== 'undefined') {
      moduleName = '../lib/' + moduleName;
      return require(moduleName);
    }
    if (typeof load !== 'undefined' && typeof java !== 'undefined') {
      moduleName = __DIR__ + '../../../main/javascript/gremlin-javascript/' + moduleName;
      var path = new java.io.File(moduleName).getCanonicalPath();
      this.__dependencies = this.__dependencies || {};
      return this.__dependencies[path] = (this.__dependencies[path] || load(path));
    }
    throw new Error('No module loader was found');
  }

  function AssertError(message) {
    Error.call(this, message);
    this.stack = (new Error(message)).stack;
    if (typeof print !== 'undefined') {
      print(this.stack);
    }
  }

  inherits(AssertError, Error);

  function inherits(ctor, superCtor) {
    ctor.super_ = superCtor;
    ctor.prototype = Object.create(superCtor.prototype, {
      constructor: {
        value: ctor,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
  }
  var toExport = {
    assert: assert,
    loadLibModule: loadLibModule
  };
  if (typeof module !== 'undefined') {
    // CommonJS
    module.exports = toExport;
    return;
  }
  return toExport;
}).call(this);