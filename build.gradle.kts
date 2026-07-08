plugins {
    id("io.hevo.gradle-plugins.connector")
}

// The io.hevo.gradle-plugins.connector plugin provides:
// - All base plugin features (Java 17, AWS S3 repos, BOM, Spotless, JaCoCo, SonarQube, Maven publishing)
// - Shadow JAR with io.hevo.connector.application.Main
// - Application plugin configuration
// - Test configuration with frameworkIT integration

val connectorFrameworkVersion = project.findProperty("connectorFrameworkVersion") as String

dependencies {
    // Hevo Framework
    implementation("io.hevo:framework:$connectorFrameworkVersion")
    implementation("io.hevo:cdk:$connectorFrameworkVersion")
    implementation("io.hevo:saas-cdk:$connectorFrameworkVersion")

    // Manifest interpretation
    implementation("com.hubspot.jinjava:jinjava:2.7.4")
    implementation("com.networknt:json-schema-validator:1.5.6")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.2")

    // Test Dependencies
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-junit-jupiter")
    testImplementation("org.mockito:mockito-core")
    testImplementation("io.hevo:frameworkIT:$connectorFrameworkVersion") // For TCK
}
