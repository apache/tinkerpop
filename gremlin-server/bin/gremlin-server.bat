:: Windows launcher script for Rexster
@echo off

cd %CD%\..\target\

set TARGET=

for /f "tokens=*" %%a in ('dir /b /ad') do (
if exist "%%a\bin\gremlin-server.bat" set TARGET=%%a
)

cd %TARGET%\bin\
call gremlin-server.bat %*