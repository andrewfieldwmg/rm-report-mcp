#!/usr/bin/env bash
set -euo pipefail
tools=${1:?pass an empty toolchain directory}
mkdir -p "$tools/jars"

fetch() {
  local url=$1 file=$2 algorithm=$3 expected=$4
  printf 'Downloading %s\n' "${file##*/}"
  curl --fail --silent --show-error --location --retry 1 --connect-timeout 12 --max-time 100 "$url" -o "$file"
  printf '%s  %s\n' "$expected" "$file" | "$algorithm" --check --strict
}

# Copy the runtime from a verified image without starting its container.
extract_distribution() (
  local image=$1 source=$2 destination=$3 container
  printf 'Pulling %s\n' "$image"
  timeout 150s docker pull --platform linux/amd64 "$image"
  container=$(docker create --network none "$image")
  trap 'docker rm "$container" >/dev/null' EXIT
  mkdir -p "$destination"
  docker cp "$container:$source/." "$destination"
)

temurin=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.17%2B10
fetch "$temurin/OpenJDK17U-jdk_x64_linux_hotspot_17.0.17_10.tar.gz" "$tools/java.tgz" sha256sum 992f96e7995075ac7636bb1a8de52b0c61d71ed3137fafc979ab96b4ab78dd75
tar --no-same-owner -xzf "$tools/java.tgz" -C "$tools"
extract_distribution docker.io/apache/spark@sha256:541b2112c59fadc44639620d77644a2a0131738ec1e7262da56f3c3137d9686e \
  /opt/spark "$tools/spark-3.5.8-bin-hadoop3"
extract_distribution docker.io/apache/kafka@sha256:29a81359d19e2fc90b38bba622244e7f85eb4c06affd975de5d1576393ef8a9d \
  /opt/kafka "$tools/kafka_2.13-4.0.2"
test -x "$tools/spark-3.5.8-bin-hadoop3/bin/spark-submit"
test -x "$tools/kafka_2.13-4.0.2/bin/kafka-server-start.sh"

jar() {
  local group=$1 name=$2 version=$3 hash=$4
  local file=$name-$version.jar
  fetch "https://repo.maven.apache.org/maven2/$group/$name/$version/$file" "$tools/jars/$file" sha256sum "$hash"
}
jar io/delta delta-spark_2.12 3.3.2 4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85
jar io/delta delta-storage 3.3.2 c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36
jar org/apache/spark spark-sql-kafka-0-10_2.12 3.5.8 d2b7068eabce2a1267103625e58efae32f7ee384db1cbc22424532ad2ed95fb4
jar org/apache/spark spark-token-provider-kafka-0-10_2.12 3.5.8 85f44359b60943d262f37d1327ebb7931fe47e0392286fe3023299e25b84b0d8
jar org/apache/kafka kafka-clients 3.4.1 9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6
jar org/apache/commons commons-pool2 2.11.1 ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83
