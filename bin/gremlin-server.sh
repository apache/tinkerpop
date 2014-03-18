#!/bin/bash

(cd `dirname $0`/../gremlin-server/ && bin/gremlin-server.sh $@)
