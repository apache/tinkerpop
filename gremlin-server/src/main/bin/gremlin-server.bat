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

:: Windows launcher script for Gremlin Server
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

set JAVA_OPTIONS=-Xms32m -Xmx512m

if "%1" == "" goto server
if "%1" == "install" goto install

:server

:: Launch the application
java -Dlog4j.configuration=conf/log4j-server.properties %JAVA_OPTIONS% %JAVA_ARGS% -cp "%LIBDIR%/*;%EXTDIR%;" org.apache.tinkerpop.gremlin.server.GremlinServer %*

:install

set RESTVAR=
shift
:loop1
if "%1"=="" goto after_loop
set RESTVAR=%RESTVAR% %1
shift
goto loop1

:after_loop

java -Dlog4j.configuration=conf/log4j-server.properties %JAVA_OPTIONS% %JAVA_ARGS% -cp "%LIBDIR%/*;%EXTDIR%;" org.apache.tinkerpop.gremlin.server.util.GremlinServerInstall %RESTVAR%
