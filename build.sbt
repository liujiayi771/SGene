name := "ngs-spark"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1" % "compile"
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.sellmerfud" % "optparse_2.11" % "2.2"

//proguardSettings
//ProguardKeys.proguardVersion in Proguard := "5.2.1"
//inConfig(Proguard)(javaOptions in ProguardKeys.proguard := Seq("-Xmx2g"))
//ProguardKeys.inputs in Proguard <<= exportedProducts in Compile map { _.files }
//ProguardKeys.options in Proguard += ProguardOptions.keepMain("edu.hust.elwg.NGSSpark")
//ProguardKeys.options in Proguard  += """
//-dontnote
//-dontwarn
//-dontusemixedcaseclassnames
//"""
//
//fullClasspath in assembly <<= (fullClasspath in assembly, exportedProducts in Compile,
//  ProguardKeys.outputs in Proguard, ProguardKeys.proguard in Proguard) map {
//  (cp, unobfuscated, obfuscated, p) =>
//    ((cp filter { !unobfuscated.contains(_) }).files ++ obfuscated).classpath
//}
//
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}