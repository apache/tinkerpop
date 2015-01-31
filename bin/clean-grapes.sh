#!/bin/bash

# This script removes the com.tinkerpop directory from the "grapes" directory. This
# script assumes that an environment variable of GROOVY_GRAPES is set with the location
# of the grapes directory.  Otherwise, it will set that value to the standard location
# of ~/.groovy/grapes which is common for most installations.  Usage:
#
# bin/clean-grapes.sh

if [ -z "${GROOVY_GRAPES:-}" ]; then
    GROOVY_GRAPES=~/.groovy/grapes
fi

GRAPES_TP="$GROOVY_GRAPES/com.tinkerpop"

rm -rf $GRAPES_TP