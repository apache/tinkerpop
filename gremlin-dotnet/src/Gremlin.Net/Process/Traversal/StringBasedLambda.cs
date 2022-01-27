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

namespace Gremlin.Net.Process.Traversal
{
    internal class StringBasedLambda : ILambda
    {
        private const int DefaultArgument = -1;

        public StringBasedLambda(string expression, string language)
        {
            LambdaExpression = expression;
            Language = language;
        }

        public string LambdaExpression { get; }

        public string Language { get; }

        public int Arguments { get; protected set; } = DefaultArgument;
    }

    internal class GroovyStringBasedLambda : StringBasedLambda
    {
        public GroovyStringBasedLambda(string expression) : base(expression, "gremlin-groovy")
        {
            // try to detect 1 or 2 argument lambda if possible otherwise go with unknown which is the default
            if (!expression.Contains("->")) return;
            var args = expression.Substring(0, expression.IndexOf("->", StringComparison.Ordinal));
            Arguments = args.Contains(",") ? 2 : 1;
        }
        
        public GroovyStringBasedLambda(string expression, int arguments) : base(expression, "gremlin-groovy")
        {
            Arguments = arguments;
        }

    }
}