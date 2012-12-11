#/bin/bash

ps ax | grep python cpl.py | awk '{print $1}' | xargs kill -9
