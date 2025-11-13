#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import platform

gremlin_version = "3.8.0"  # DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

def _generate_user_agent():
    application_name = "NotAvailable"
    runtime_version = platform.python_version().replace(" ", "_")
    os_name = platform.system().replace(" ", "_")
    os_version = platform.release().replace(" ", "_")
    architecture = platform.machine().replace(" ", "_")
    user_agent = "{appName} Gremlin-Python.{driverVersion} {runtimeVersion} {osName}.{osVersion} {cpuArch}".format(
                    appName=application_name, driverVersion=gremlin_version, runtimeVersion=runtime_version,
                    osName=os_name, osVersion=os_version, cpuArch=architecture)

    return user_agent


userAgent = _generate_user_agent()
userAgentHeader = "User-Agent"
