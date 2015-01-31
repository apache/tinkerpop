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
if "%1" == "-i" goto install

:server

:: Launch the application
java -Dlog4j.configuration=conf/log4j-server.properties %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*;%EXTDIR%; com.tinkerpop.gremlin.server.GremlinServer %*

:install

set RESTVAR=
shift
:loop1
if "%1"=="" goto after_loop
set RESTVAR=%RESTVAR% %1
shift
goto loop1

:after_loop

java -Dlog4j.configuration=conf/log4j-server.properties %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*;%EXTDIR%; com.tinkerpop.gremlin.server.util.GremlinServerInstall %RESTVAR%