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
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class BulkSetSerializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            var jArray = graphsonObject as JArray;
            if (jArray == null)
            {
                return new List<object>(0);
            }
            
            // coerce the BulkSet to List. if the bulk exceeds the int space then we can't coerce to List anyway, 
            // so this query will be trouble. we'd need a legit BulkSet implementation here in C#. this current 
            // implementation is here to replicate the previous functionality that existed on the server side in 
            // previous versions.
            var result = new List<Tuple<dynamic,int>>();
            for (var i = 0; i < jArray.Count; i += 2)
            {
                result.Add(new Tuple<dynamic,int>(reader.ToObject(jArray[i]), (int) reader.ToObject(jArray[i + 1])));
            }
            
            return result.Select(x => Enumerable.Repeat(x.Item1, x.Item2)).SelectMany<dynamic,dynamic>(x => x).ToList();
        }
    }
}