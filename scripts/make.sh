#!/bin/bash

./gradlew makeJar copyDependencies

mkdir -p out/bin 2>/dev/null

cp build/libs/bch-scaling-*.jar out/bin/main.jar
cp -R build/libs/libs out/bin/.
