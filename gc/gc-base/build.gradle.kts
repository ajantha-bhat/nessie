/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - Base Implementation"

val sparkScala = useSparkScalaVersionsForProject("3.2")

dependencies {
  implementation(platform(nessieRootProject()))
  annotationProcessor(platform(nessieRootProject()))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))

  forScala(sparkScala.scalaVersion)

  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  implementation(nessieClientForIceberg())
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.google.code.findbugs:jsr305")

  compileOnly("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  compileOnly(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
  )

  testImplementation(platform(nessieRootProject()))

  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  testImplementation(nessieProject("nessie-jaxrs-testextension")) {
    exclude("org.projectnessie", "nessie-client")
    exclude("org.projectnessie", "nessie-model")
  }
  testImplementation(nessieProject("nessie-jaxrs-tests")) {
    exclude("org.projectnessie", "nessie-client")
    exclude("org.projectnessie", "nessie-model")
  }
  testImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
    exclude("com.sun.jersey", "jersey-servlet")
  }
  testImplementation("org.slf4j:log4j-over-slf4j")
  testImplementation("ch.qos.logback:logback-classic")

  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
  )
  testImplementation(project(":nessie-spark-extensions-base_${sparkScala.scalaMajorVersion}")) {
    testJarCapability()
  }
  testImplementation(
    project(
      ":nessie-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
    )
  )
  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

forceJava11ForTests()
