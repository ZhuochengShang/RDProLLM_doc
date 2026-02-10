@echo off
rem Usage:
rem beast-shell: Starts Spark shell with Beast classes
rem beast-shell --jar <custom jar>: Starts Spark shell with Beast classes and the given JAR file
rem beast-shell --conf1 value1 --conf2 value2 ... :
rem                 Starts Spark shell with Beast classes and pass additional spark-shell configuration
setlocal enabledelayedexpansion

rem Define the version for the beast-spark package
set "beast_spark_version=0.10.2-SNAPSHOT"

rem Get the directory where the script is located
set "script_dir=%~dp0"
set "lib_dir=%script_dir%..\lib\"

set "startup-scala=%script_dir%startup-script.scala"
if not exist !startup-scala! (
  echo Creating !startup-scala!
  (
    echo println^(
    echo   """
    echo     ^|                 ___   ___       ___ ___
    echo     ^|  Empowered by:   __^) ^|__   /\  /__   ^|
    echo     ^|                 ^|__^) ^|___ /__\ ___/  ^|    version %beast_spark_version%
    echo     ^|  Visit https://bitbucket.org/bdlabucr/beast/src/master/doc for more details
    echo     ^|""")
    echo import edu.ucr.cs.bdlab.beast._
    echo org.apache.spark.beast.CRSServer.startServer^(sc^)
    echo org.apache.spark.beast.SparkSQLRegistration.registerUDT
    echo org.apache.spark.beast.SparkSQLRegistration.registerUDF^(spark^)
  ) > !startup-scala!
)

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
set "spark_exclude_packages=org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-hdfs,javax.media:jai_core"

set "spark_repositories=https://repo.osgeo.org/repository/release/"
set "spark_args="

rem Process special arguments --jars, --packages, and --repositories
:parse_args
if "%~1" == "" goto run_spark_submit
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
set "spark_args=!spark_args! %~1"
shift
goto parse_args

:run_spark_submit
rem Construct the spark-submit command with all arguments
set "spark_submit_command=spark-shell"
set "spark_submit_arguments=--jars !spark_jars!"
set "spark_submit_arguments=!spark_submit_arguments! --packages !spark_packages!"
set "spark_submit_arguments=!spark_submit_arguments! --repositories !spark_repositories!"
set "spark_submit_arguments=!spark_submit_arguments! --exclude-packages !spark_exclude_packages!"
set "spark_submit_arguments=!spark_submit_arguments! -I !startup-scala!"
set "spark_submit_arguments=!spark_submit_arguments! !spark_args!"

echo Running the following command:
echo !spark_submit_command! !spark_submit_arguments!

rem Run the spark-submit command
!spark_submit_command! !spark_submit_arguments!

endlocal
