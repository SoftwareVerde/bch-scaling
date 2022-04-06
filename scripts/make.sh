#!/bin/bash

./gradlew makeJar

mkdir -p out/bin 2>/dev/null

cp build/libs/bch-scaling-*.jar out/bin/main.jar
