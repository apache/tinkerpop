#!/bin/bash

(cd `dirname $0`/../gremlin/gremlin-server/ && bin/gremlin-server.sh $@)
