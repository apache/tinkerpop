#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.IO;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class ClassSerializer : IGraphSONSerializer
    {
        public Dictionary<string, dynamic> Dictify(dynamic objectData, GraphSONWriter writer)
        {
            var type = (Type) objectData;

            // try to create the Type and then access the Fqcn property. if that's not there then
            // throw an exception
            var instance = Activator.CreateInstance(type) 
                           ?? throw new IOException($"Cannot create an instance of {type.FullName}");
            var fqcnProperty = type.GetProperty("Fqcn") 
                               ?? throw new IOException($"The type {type.FullName} does not have a 'Fqcn' property");
            var fqcnValue = fqcnProperty.GetValue(instance) 
                            ?? throw new IOException($"The 'Fqcn' property of {type.FullName} is null");
            return GraphSONUtil.ToTypedValue("Class", fqcnValue);
        }
    }
}