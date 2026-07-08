plugins {
    id("io.hevo.gradle-plugins.connector")
}

// The io.hevo.gradle-plugins.connector plugin provides:
// - All base plugin features (Java 17, AWS S3 repos, BOM, Spotless, JaCoCo, SonarQube, Maven publishing)
// - Shadow JAR with io.hevo.connector.application.Main
// - Application plugin configuration
// - Test configuration with frameworkIT integration

val connectorFrameworkVersion = project.findProperty("connectorFrameworkVersion") as String

// Configuration for framework sources extraction
val cdkSourcesConfig: Configuration by configurations.creating {
    isTransitive = false
}

val saasCdkSourcesConfig: Configuration by configurations.creating {
    isTransitive = false
}

dependencies {
    // Hevo Framework
    implementation("io.hevo:framework:$connectorFrameworkVersion")
    implementation("io.hevo:cdk:$connectorFrameworkVersion")
    implementation("io.hevo:saas-cdk:$connectorFrameworkVersion")

    // Manifest interpretation
    implementation("com.hubspot.jinjava:jinjava:2.7.4")
    implementation("com.networknt:json-schema-validator:1.5.6")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.2")

    // Framework Sources for reference
    cdkSourcesConfig("io.hevo:cdk:$connectorFrameworkVersion:sources")
    saasCdkSourcesConfig("io.hevo:saas-cdk:$connectorFrameworkVersion:sources")

    // Test Dependencies
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-junit-jupiter")
    testImplementation("org.mockito:mockito-core")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("io.hevo:frameworkIT:$connectorFrameworkVersion") // For TCK
}

tasks.test {
    // The declarative component schema compilation is memory-hungry; give tests headroom.
    maxHeapSize = "1g"
}

// Extract framework sources for reference
tasks.register<Sync>("extractFrameworkSources") {
    notCompatibleWithConfigurationCache("Uses zipTree which captures script references")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(cdkSourcesConfig.elements.map { files -> files.map { zipTree(it) } })
    from(saasCdkSourcesConfig.elements.map { files -> files.map { zipTree(it) } })
    into(layout.buildDirectory.dir("sources/framework"))
}
