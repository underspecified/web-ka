#/bin/bash

ps ax | grep python espresso.py | awk '{print $1}' | xargs kill -9
