#!/bin/bash

cd out

exec java -XX:MaxRAMPercentage=75.0 -XX:+UseG1GC -XX:NewSize=128M -XX:MaxNewSize=128M -XX:+UnlockExperimentalVMOptions -XX:InitiatingHeapOccupancyPercent=20 -XX:G1OldCSetRegionThresholdPercent=90 -XX:G1MixedGCLiveThresholdPercent=50 -XX:MaxGCPauseMillis=5000 -jar bin/main.jar "$@"

