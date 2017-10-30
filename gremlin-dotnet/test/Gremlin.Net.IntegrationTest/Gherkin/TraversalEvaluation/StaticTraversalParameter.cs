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
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    internal class StaticTraversalParameter : ITokenParameter, IEquatable<StaticTraversalParameter>
    {
        private readonly string _traversalText;

        public bool Equals(StaticTraversalParameter other)
        {
            return Tokens.SequenceEqual(other.Tokens);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((StaticTraversalParameter) obj);
        }

        public override int GetHashCode()
        {
            return Tokens != null ? Tokens.GetHashCode() : 0;
        }

        public object GetValue()
        {
            return TraversalParser.GetTraversalFromTokens(Tokens, null, _traversalText);
        }

        public Type GetParameterType()
        {
            return typeof(ITraversal);
        }

        public IList<Token> Tokens { get; }
        
        public StaticTraversalParameter(IList<Token> tokens, string traversalText)
        {
            _traversalText = traversalText;
            Tokens = tokens;
        }
    }
}