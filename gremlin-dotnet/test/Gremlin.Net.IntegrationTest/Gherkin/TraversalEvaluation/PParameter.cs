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
    /// Represents a parameter for a traversal predicate (ie: P.gt())
    /// </summary>
    internal class PParameter : ITokenParameter, IEquatable<PParameter>
    {
        private IDictionary<string, object> _contextParameterValues;
        public IList<Token> Tokens { get; }
        
        public PParameter(IList<Token> tokens)
        {
            Tokens = tokens;
        }

        public bool Equals(PParameter other)
        {
            return Tokens.SequenceEqual(other.Tokens);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((PParameter) obj);
        }

        public override int GetHashCode()
        {
            return Tokens != null ? Tokens.GetHashCode() : 0;
        }

        public object GetValue()
        {
            var type = typeof(P);
            object instance = null;
            for (var i = 1; i < Tokens.Count; i++)
            {
                var token = Tokens[i];
                token.SetContextParameterValues(_contextParameterValues);
                var method = type.GetMethod(TraversalParser.GetCsharpName(token.Name),
                    BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public);
                if (method == null)
                {
                    throw new InvalidOperationException($"Predicate (P) method '{token}' not found for testing");
                }
                
                var parameters = method.IsStatic
                    ? new object[] {token.Parameters.Select(p => p.GetValue()).ToArray()}
                    : token.Parameters.Select(p => p.GetValue()).ToArray();
                instance = method.Invoke(instance, parameters);
            }
            return instance;
        }

        public Type GetParameterType()
        {
            return typeof(P);
        }

        public void SetContextParameterValues(IDictionary<string, object> parameterValues)
        {
            _contextParameterValues = parameterValues;
        }
    }
}