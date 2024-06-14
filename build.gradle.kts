plugins {
    kotlin("jvm") version "1.9.23"
    id("eu.kakde.gradle.sonatype-maven-central-publisher") version "1.0.6"
    `maven-publish`
    signing
}

group = "io.github.holydrug"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api("tech.ytsaurus:ytsaurus-client:1.2.1")

    testImplementation("javax.persistence:persistence-api:1.0.2")
    testImplementation(kotlin("test"))
}

// ------------------------------------
// PUBLISHING TO SONATYPE CONFIGURATION
// ------------------------------------
object Meta {
  val COMPONENT_TYPE = "java" // "java" or "versionCatalog"
  val GROUP = "io.github.holydrug"
  val ARTIFACT_ID = "ytsaurus-query-builder"
  val VERSION = "1.0.1"
  val PUBLISHING_TYPE = "AUTOMATIC" // USER_MANAGED or AUTOMATIC
  val SHA_ALGORITHMS = listOf("SHA-256", "SHA-512") // sha256 and sha512 are supported but not mandatory. Only sha1 is mandatory but it is supported by default.
  val DESC = "ytsaurus-query-builder is an internal DSL and source code generator, modelling the YQL language as a type safe Java API to help you write better YQL."
  val LICENSE = "Apache-2.0"
  val LICENSE_URL = "https://opensource.org/licenses/Apache-2.0"
  val GITHUB_REPO = "holydrug/ytsaurus-query-builder.git"
  val DEVELOPER_ID = "holydrug"
  val DEVELOPER_NAME = "Sergey Popov Romanovich"
}

val sonatypeUsername: String? by project // this is defined in ~/.gradle/gradle.properties
val sonatypePassword: String? by project // this is defined in ~/.gradle/gradle.properties

sonatypeCentralPublishExtension {
  // Set group ID, artifact ID, version, and other publication details
  groupId.set(Meta.GROUP)
  artifactId.set(Meta.ARTIFACT_ID)
  version.set(Meta.VERSION)
  componentType.set(Meta.COMPONENT_TYPE) // "java" or "versionCatalog"
  publishingType.set(Meta.PUBLISHING_TYPE) // USER_MANAGED or AUTOMATIC

  // Set username and password for Sonatype repository
  username.set(System.getenv("SONATYPE_USERNAME") ?: sonatypeUsername)
  password.set(System.getenv("SONATYPE_PASSWORD") ?: sonatypePassword)

  // Configure POM metadata
  pom {
    name.set(Meta.ARTIFACT_ID)
    description.set(Meta.DESC)
    url.set("https://github.com/${Meta.GITHUB_REPO}")
    licenses {
      license {
        name.set(Meta.LICENSE)
        url.set(Meta.LICENSE_URL)
      }
    }
    developers {
      developer {
        id.set(Meta.DEVELOPER_ID)
        name.set(Meta.DEVELOPER_NAME)
      }
    }
    scm {
      url.set("https://github.com/${Meta.GITHUB_REPO}")
      connection.set("scm:git:https://github.com/${Meta.GITHUB_REPO}")
      developerConnection.set("scm:git:https://github.com/${Meta.GITHUB_REPO}")
    }
    issueManagement {
      system.set("GitHub")
      url.set("https://github.com/${Meta.GITHUB_REPO}/issues")
    }
  }
}


tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}