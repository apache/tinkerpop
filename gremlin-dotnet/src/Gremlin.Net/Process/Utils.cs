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
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using static System.Runtime.InteropServices.RuntimeInformation;

namespace Gremlin.Net.Process
{
    /// <summary>
    /// Contains useful methods that can be reused across the project. 
    /// </summary>
    internal static class Utils
    {
        /// <summary>
        /// The user agent which is sent with connection requests if enabled.
        /// </summary>
        public static string UserAgent => _userAgent ??= GenerateUserAgent();
        private static string? _userAgent;
        
        /// <summary>
        /// Waits the completion of the provided Task.
        /// When an AggregateException is thrown, the inner exception is thrown.
        /// </summary>
        public static void WaitUnwrap(this Task task)
        {
            try
            {
                task.Wait();
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException != null)
                {
                    ExceptionDispatchInfo.Capture(ex.InnerException).Throw();   
                }
                throw;
            }
        }
        
        /// <summary>
        /// Designed for Tasks that were started but the result should not be awaited upon (fire and forget).
        /// </summary>
        public static void Forget(this Task task)
        {
            // Avoid compiler warning CS4014 and Unobserved exceptions
            task?.ContinueWith(t =>
            {
                t.Exception?.Handle(_ => true);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        ///  Returns a user agent for connection request headers.
        ///
        /// Format:
        /// "[Application Name] [GLV Name].[Version] [Language Runtime Version] [OS].[Version] [CPU Architecture]"
        /// </summary>
        private static string GenerateUserAgent()
        {
            var applicationName = Assembly.GetEntryAssembly()?.GetName().Name?
                                                        .Replace(' ', '_') ?? "NotAvailable";
            var driverVersion = Tokens.GremlinVersion;
            var languageVersion = Environment.Version.ToString().Replace(' ', '_');
            var osName = Environment.OSVersion.Platform.ToString().Replace(' ', '_');
            var osVersion = Environment.OSVersion.Version.ToString().Replace(' ', '_');
            var cpuArchitecture = OSArchitecture.ToString().Replace(' ', '_');
            
            return $"{applicationName} gremlin-dotnet.{driverVersion} {languageVersion} {osName}.{osVersion} {cpuArchitecture}";
        }
    }
}
