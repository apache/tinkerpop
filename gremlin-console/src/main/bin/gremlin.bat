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

@echo off
SETLOCAL EnableDelayedExpansion
set work=%CD%

if [%work:~-3%]==[bin] cd ..

set LIBDIR=lib
set EXTDIR=ext/*

cd ext

FOR /D /r %%i in (*) do (
    set EXTDIR=!EXTDIR!;%%i/*
)

cd ..

set GREMLIN_LOG_LEVEL=WARN

:: Process options

:parse
IF "%~1"=="" GOTO endparse
IF "%~1"=="-l" set GREMLIN_LOG_LEVEL=%~2
SHIFT
GOTO parse
:endparse

:: workaround for https://issues.apache.org/jira/browse/GROOVY-6453
set JAVA_OPTIONS=-Xms32m -Xmx512m -Djline.terminal=none

:: Launch the application

java %JAVA_OPTIONS% %JAVA_ARGS% -cp "%LIBDIR%\*;%EXTDIR%;" "-Dlogback.configurationFile=conf/logback.xml" "-Dgremlin.logback.level=%GREMLIN_LOG_LEVEL%" org.apache.tinkerpop.gremlin.console.Console %*

set CLASSPATH=%OLD_CLASSPATH%