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

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    public class ContextBasedParameter : ITokenParameter, IEquatable<ContextBasedParameter>
    {
        public bool Equals(ContextBasedParameter other)
        {
            return string.Equals(_name, other._name) && Equals(_value, other._value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ContextBasedParameter) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((_name != null ? _name.GetHashCode() : 0) * 397) ^ (_value != null ? _value.GetHashCode() : 0);
            }
        }

        private readonly string _name;
        private object _value;

        public ContextBasedParameter(string name)
        {
            _name = name;
        }

        public void SetContextParameterValues(IDictionary<string, object> parameterValues)
        {
            if (parameterValues == null || !parameterValues.TryGetValue(_name, out var value))
            {
                throw new InvalidOperationException($"Parameter \"{_name}\" was not provided");
            }
            _value = value;
        }
        
        public object GetValue()
        {
            return _value;
        }

        public Type GetParameterType()
        {
            if (_value == null)
            {
                throw new NullReferenceException($"Value for parameter \"{_name}\" was not set");
            }
            return _value.GetType();
        }
    }
}