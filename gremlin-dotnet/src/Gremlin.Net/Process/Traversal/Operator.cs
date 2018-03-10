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

// THIS IS A GENERATED FILE - DO NOT MODIFY THIS FILE DIRECTLY - see pom.xml
namespace Gremlin.Net.Process.Traversal
{
#pragma warning disable 1591

    public class Operator : EnumWrapper
    {
        private Operator(string enumValue)
            : base("Operator", enumValue)
        {            
        }

        public static Operator AddAll => new Operator("addAll");
		public static Operator And => new Operator("and");
		public static Operator Assign => new Operator("assign");
		public static Operator Div => new Operator("div");
		public static Operator Max => new Operator("max");
		public static Operator Min => new Operator("min");
		public static Operator Minus => new Operator("minus");
		public static Operator Mult => new Operator("mult");
		public static Operator Or => new Operator("or");
		public static Operator Sum => new Operator("sum");
		public static Operator SumLong => new Operator("sumLong");
    }
    
#pragma warning restore 1591
}