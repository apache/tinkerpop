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
using System.Reflection;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    /// <summary>
    /// Represents a parameter for a traversal token (ie: T.label)
    /// </summary>
    internal class TraversalEnumParameter : ITokenParameter, IEquatable<TraversalEnumParameter>
    {
        private readonly string _text;

        private static readonly IDictionary<string, Type> EnumTypesByName = typeof(Scope).GetTypeInfo().Assembly
            .GetTypes().Where(t => t.GetTypeInfo().IsSubclassOf(typeof(EnumWrapper)) && t.GetTypeInfo().IsPublic)
            .ToDictionary(e => e.Name, e => e);
        
        private readonly object _value;
        private readonly Type _type;
        
        public TraversalEnumParameter(string text)
        {
            _text = text;
            var separatorIndex = text.IndexOf('.');
            var enumTypeName = text.Substring(0, separatorIndex);
            if (!EnumTypesByName.TryGetValue(enumTypeName, out var type))
            {
                throw new KeyNotFoundException($"Enum with name {enumTypeName} not found");
            }
            _type = type;
            var valueName = text.Substring(separatorIndex + 1);
            _value = _type.GetProperty(GetCsharpName(valueName)).GetValue(null);
        }

        private string GetCsharpName(string valueText)
        {
            if (_type == typeof(Direction))
            {
                valueText = valueText.ToLower();
            }
            return TraversalParser.GetCsharpName(valueText);
        }

        public bool Equals(TraversalEnumParameter other)
        {
            return _text == other._text;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TraversalEnumParameter) obj);
        }

        public override int GetHashCode()
        {
            return _text.GetHashCode();
        }

        public object GetValue()
        {
            return _value;
        }

        public Type GetParameterType()
        {
            return _type;
        }

        public void SetContextParameterValues(IDictionary<string, object> parameterValues)
        {

        }
    }
}