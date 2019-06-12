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
using System.Dynamic;
using System.Linq;
using System.Reflection;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    /// <summary>
    /// Represents a parameter for the io() step - (e.g. IO.graphml)
    /// </summary>
    internal class IOParameter : ITokenParameter, IEquatable<IOParameter>
    {
        private readonly string _text;
        private readonly string _value;
        
        public IOParameter(string text)
        {
            _text = text;
            var separatorIndex = text.IndexOf('.');
            _value = text.Substring(separatorIndex + 1);
        }

        public bool Equals(IOParameter other)
        {
            return _text == other._text;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((IOParameter) obj);
        }

        public override int GetHashCode()
        {
            return _text.GetHashCode();
        }

        public object GetValue()
        {
            var field = typeof(IO).GetField(_value, BindingFlags.Static | BindingFlags.Public);
            return field.GetValue(null);
        }

        public void SetContextParameterValues(IDictionary<string, object> parameterValues)
        {

        }

        public Type GetParameterType()
        {
            return typeof(String);
        }
    }
}