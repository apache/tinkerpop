:: Licensed to the Apache Software Foundation (ASF) under one
:: or more contributor license agreements.  See the NOTICE file
:: distributed with this work for additional information
:: regarding copyright ownership.  The ASF licenses this file
:: to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance
:: with the License.  You may obtain a copy of the License at
::
::   http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing,
:: software distributed under the License is distributed on an
:: "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
:: KIND, either express or implied.  See the License for the
:: specific language governing permissions and limitations
:: under the License.

:: Windows launcher script for Gremlin Console
:: Use this with Java8 as it contains a workaround for Windows Path Loading for Java8

@echo off
SETLOCAL EnableDelayedExpansion

set work=%CD%
if [%work:~-3%]==[bin] cd ..
set work=%CD%

set CONSOLE_JARS=

FOR %%i in (.\lib\*.jar .\lib\*.JAR) do (
    :: get lib jars with a relative path as there is a hard limit to length of a variable
    set CONSOLE_JARS=!CONSOLE_JARS!;%%i
)

cd ext
FOR /r %%j in (*.jar *.JAR) do (
    :: get all ext jars with a relative path as there is a hard limit to length of a variable
    SET FULL_PATH=%%j
    set CONSOLE_JARS=!CONSOLE_JARS!;.!FULL_PATH:%work%=!
)
cd ..

:: workaround for https://issues.apache.org/jira/browse/GROOVY-6453
set JAVA_OPTIONS=-Xms32m -Xmx512m -Djline.terminal=none

:: Launch the application

java %JAVA_OPTIONS% %JAVA_ARGS% -cp "%CONSOLE_JARS%" org.apache.tinkerpop.gremlin.console.Console %*
