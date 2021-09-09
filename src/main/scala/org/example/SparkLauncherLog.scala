package org.example

import org.apache.spark.launcher.SparkLauncher

object SparkLauncherLog {

  val launcher: SparkLauncher = new SparkLauncher()
    .setVerbose(true)
    .setMaster("yarn")
    .setDeployMode("local")
    .setConf("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
    //.setMainClass(config.mainClass)
    //.setAppResource(config.sparkJar)
    .addSparkArg("--files", "/DEVPI_DE/ebip/dp/utl-clustersanity/current/conf/log4j.properties")
    //.addSparkArg("--jars", config.jars)

}
