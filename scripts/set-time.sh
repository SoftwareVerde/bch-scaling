#!/bin/bash

if [[ "$(uname)" -eq "Linux" ]]; then
        sudo date --set 2022-04-27
        sudo date --set 22:00:00
else
        sudo systemsetup -setusingnetworktime Off 2>/dev/null
        sudo systemsetup -setdate 04/27/22 2>/dev/null
        sudo systemsetup -settime 22:00:00 2>/dev/null
fi