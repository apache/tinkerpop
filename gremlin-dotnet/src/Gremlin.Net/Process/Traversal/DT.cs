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

namespace Gremlin.Net.Process.Traversal
{
#pragma warning disable 1591

    public class DT : EnumWrapper, IFunction
    {
        private DT(string enumValue)
            : base("DT", enumValue)
        {
        }

        public static DT Second => new DT("second");
        public static DT Minute => new DT("minute");
        public static DT Hour => new DT("hour");
        public static DT Day => new DT("day");


        private static readonly Dictionary<string, DT> Properties = new()
        {
            { "second", Second },
            { "minute", Minute },
            { "hour", Hour },
            { "day", Day },
        };

        /// <summary>
        /// Gets the Merge enumeration by value.
        /// </summary>
        public static DT GetByValue(string value)
        {
            if (!Properties.TryGetValue(value, out var property))
            {
                throw new ArgumentException($"No matching DT for value '{value}'");
            }
            return property;
        }
    }


#pragma warning restore 1591
}