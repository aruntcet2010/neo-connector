#!/bin/zsh
set -o errexit
set -o pipefail

version=$(grep "^version=" gradle.properties | cut -d'=' -f2)
echo "Version: $version"

echo "Running spotlessApply"
./gradlew spotlessApply

# build
echo "Building the project"
./gradlew clean build -x test

docker_image_name="neo-connector-service"

# Build neo-connector Image
echo "building ${docker_image_name} docker image"
docker build -f ./Dockerfile.local --build-arg VERSION="${version}" -t "${docker_image_name}" .

echo "registering docker image with h20"
h20 register-local-connectors "${docker_image_name}"
