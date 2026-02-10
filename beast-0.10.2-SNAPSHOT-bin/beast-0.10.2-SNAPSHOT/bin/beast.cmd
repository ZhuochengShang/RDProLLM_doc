@echo off

rem Usage:
rem beast - Runs Beast main which lists the available commands
rem beast <command> <arguments> - Runs the given standard command with arguments
rem beast --jar <custom jar> <command> <arguments> - Runs either a standard command or a custom command defined
rem        in the given JAR
rem beast --class <main class> --jar <custom jar> <arguments> - Runs a user-provided main class from the given JAR
rem        file with the given arguments
rem beast --conf value <command> <arguments> - Runs the given standard Beast command and pass additional configuration
rem        to spark-submit command, e.g., master, packages, jars, ... etc.

setlocal enabledelayedexpansion

rem Define the version for the beast-spark package
set "beast_spark_version=0.10.2-SNAPSHOT"

rem Get the directory where the script is located
set "script_dir=%~dp0"
set "lib_dir=%script_dir%..\lib\"

set "spark_jars="

for %%i in ("%lib_dir%*.jar") do (
    if "!spark_jars!" == "" (
        set "spark_jars=%%i"
    ) else (
        set "spark_jars=!spark_jars!,%%i"
    )
)

rem Initialize default packages and repositories
set "spark_packages=org.mortbay.jetty:jetty:6.1.26,org.eclipse.jetty:jetty-servlet:9.4.48.v20220622,org.eclipse.jetty:jetty-server:9.4.48.v20220622,org.geotools:gt-epsg-hsql:26.1,org.geotools:gt-coverage:26.1,org.locationtech.jts:jts-core:1.19.0,com.h2database:h2:2.2.224,com.google.protobuf:protobuf-java:3.21.12"

set "spark_repositories=https://repo.osgeo.org/repository/release/"
set "spark_exclude_packages=org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-hdfs,javax.media:jai_core"
set "spark_args="

rem Process special arguments --jars, --packages, and --repositories
:parse_args
if "%~1" == "" goto prepare_program_name
if "%~1" == "--jars" (
    if "!spark_jars!" == "" (
        set "spark_jars=%~2"
    ) else (
        set "spark_jars=!spark_jars!,%~2"
    )
    shift
    shift
    goto parse_args
)
if "%~1" == "--packages" (
    set "spark_packages=!spark_packages!,%~2"
    shift
    shift
    goto parse_args
)
if "%~1" == "--repositories" (
    set "spark_repositories=!spark_repositories!,%~2"
    shift
    shift
    goto parse_args
)
if "%~1" == "--exclude-packages" (
    set "spark_exclude_packages=!spark_exclude_packages!,%~2"
    shift
    shift
    goto parse_args
)
set "argument=%~1"
if "!argument:~0,2!"=="--" (
    set "spark_args=!spark_args! %~1 %~2"
    shift
    shift
    goto parse_args
)

rem At this point, all flags have been processed. Accumulate all remaining arguments.
set "additional_args=%*"

:prepare_program_name
rem Determine if the first additional argument is a jar file and set program_name accordingly
for %%a in (!additional_args!) do (
    set "first_arg=%%~a"
    goto check_jar
)
:check_jar
if "!first_arg:~-4!" == ".jar" (
    set "program_name=!first_arg!"
    set "additional_args=!additional_args:*!first_arg!=!"
) else (
    rem Add default class if the user has not specified one
    set "program_name=%lib_dir%beast-spark-*.jar"
)

:run_spark_submit
rem Construct the spark-submit command with all arguments
set "spark_submit_command=spark-submit"
set "spark_submit_arguments=--jars !spark_jars!"
set "spark_submit_arguments=!spark_submit_arguments! --packages !spark_packages!"
set "spark_submit_arguments=!spark_submit_arguments! --repositories !spark_repositories!"
set "spark_submit_arguments=!spark_submit_arguments! --exclude-packages !spark_exclude_packages!"

set "spark_submit_arguments=!spark_submit_arguments! !spark_args!"

echo Running the following command:
echo !spark_submit_command! !spark_submit_arguments! %program_name% !additional_args!

rem Run the spark-submit command
!spark_submit_command! !spark_submit_arguments! %program_name% !additional_args!

endlocal
