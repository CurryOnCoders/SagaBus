@ECHO OFF
SETLOCAL
SET VERSION=%1
SET NUGET=nuget.exe

FOR /r %%f IN (*.nuspec) DO (
  echo "packing..."
  %NUGET% pack %%f -IncludeReferencedProjects
)

PAUSE