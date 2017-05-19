@echo off
call mvn clean
call mvn package
set /p usr="Username: "
echo user %usr%> ftpcmd.dat
set /p pwd="Password: "
echo %pwd%>> ftpcmd.dat
echo bin>> ftpcmd.dat
echo put target/nphrases.war>>ftpcmd.dat
echo quit>>ftpcmd.dat
ftp -n -s:ftpcmd.dat 192.168.0.87
del ftpcmd.dat