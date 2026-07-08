pluginManagement {
    val gradleBomVersion: String by settings

    plugins {
        id("io.hevo.gradle-plugins.connector") version gradleBomVersion
    }

    fun downloadCredentials(): Map<String, String> {
        val isCI = System.getenv("CIRCLECI") == "true"
        val url: java.net.URL =
            if (isCI) {
                val urlString = System.getenv("HEVO_M2_WAGON_AUTH_URL")
                java.net.URL(urlString)
            } else {
                java.net.URL("https://aws-key.hevo.me/mvn-read")
            }

        val awsResponseStr =
            if (isCI) {
                val urlConnection = url.openConnection()
                val userCredentials = "circleci:" + System.getenv("HEVO_M2_WAGON_PASSWORD")
                val basicAuth =
                    "Basic " +
                        java.util.Base64
                            .getEncoder()
                            .encodeToString(userCredentials.toByteArray())
                urlConnection.setRequestProperty("Authorization", basicAuth)
                urlConnection.inputStream.bufferedReader().use { it.readText() }
            } else {
                url.readText()
            }

        val jsonSlurper = groovy.json.JsonSlurper()
        @Suppress("UNCHECKED_CAST")
        return jsonSlurper.parseText(awsResponseStr) as Map<String, String>
    }

    val awsResponseJson = downloadCredentials()
    val baseMavenURL = "s3://hevo-artifacts/package/mvn"
    val releaseRepoURL = "$baseMavenURL/release"
    val snapshotRepoURL = "$baseMavenURL/snapshot"

    repositories {
        maven {
            name = "hevoMavenRelease"
            url = uri(releaseRepoURL)
            credentials(AwsCredentials::class) {
                accessKey = awsResponseJson["access_key"].orEmpty()
                secretKey = awsResponseJson["secret_key"].orEmpty()
                sessionToken = awsResponseJson["token"].orEmpty()
            }
        }
        maven {
            name = "hevoMavenSnapshot"
            url = uri(snapshotRepoURL)
            credentials(AwsCredentials::class) {
                accessKey = awsResponseJson["access_key"].orEmpty()
                secretKey = awsResponseJson["secret_key"].orEmpty()
                sessionToken = awsResponseJson["token"].orEmpty()
            }
        }
        gradlePluginPortal()
        mavenCentral()
    }
}

// Disable caching for SNAPSHOT dependencies
// Ensures connectorFrameworkVersion and gradleBomVersion SNAPSHOTs are always fetched fresh
gradle.allprojects {
    buildscript {
        configurations.all {
            resolutionStrategy.cacheChangingModulesFor(0, "seconds")
            resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
        }
    }
    configurations.all {
        resolutionStrategy.cacheChangingModulesFor(0, "seconds")
        resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
    }
}

rootProject.name = "neo-connector"
