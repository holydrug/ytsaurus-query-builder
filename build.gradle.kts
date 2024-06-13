plugins {
    kotlin("jvm") version "1.9.23"
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

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}