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
 * @author Igor Ostapenko
 */
'use strict';

module.exports = class {

  constructor(ioc) {
    this.ioc = ioc;

    // TODO: align with Java.parse(GraphSON) logic
    this.serializers = [ // specifically ordered, the first canBeUsedFor=true wins
      ioc.intSerializer,
      ioc.bytecodeSerializer,
      ioc.mapSerializer,
      ioc.uuidSerializer,
      ioc.stringSerializer,
    ];
  }

  getSerializerCanBeUsedFor(item) {
    for (let i = 0; i < this.serializers.length; i++)
      if (this.serializers[i].canBeUsedFor(item))
        return this.serializers[i];

    return this.ioc.StringSerializer; // TODO: is it what we want with falling back to a string?
  }

  serialize(item, fullyQualifiedFormat = true) {
    return this
      .getSerializerCanBeUsedFor(item)
      .serialize(item, fullyQualifiedFormat);
  }

}
