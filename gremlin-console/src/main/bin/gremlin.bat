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

set JAVA_OPTIONS=-Xms32m -Xmx512m

:: Launch the application

if "%1" == "" goto console
if "%1" == "-e" goto script
if "%1" == "-v" goto version

:console

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*;%EXTDIR%; com.tinkerpop.gremlin.console.Console %*

set CLASSPATH=%OLD_CLASSPATH%
goto :eof

:script

set strg=

FOR %%X IN (%*) DO (
CALL :concat %%X %1 %2
)

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*;%EXTDIR%; com.tinkerpop.gremlin.groovy.jsr223.ScriptExecutor %strg%
goto :eof

:version
java %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*;%EXTDIR%; com.tinkerpop.gremlin.util.Gremlin

goto :eof

:concat

if %1 == %2 goto skip

SET strg=%strg% %1

:concatsep

if "%CP%" == "" (
set CP=%LIBDIR%\%1
)else (
set CP=%CP%;%LIBDIR%\%1
)

:skip
