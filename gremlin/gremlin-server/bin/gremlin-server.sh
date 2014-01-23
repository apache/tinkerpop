#!/bin/bash

(cd `dirname $0`/../target/gremlin-server-*-standalone/ && bin/gremlin-server.sh $@)
