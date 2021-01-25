#!/bin/bash

for branch in $(git branch -r | grep -Po 'TINKERPOP[3]?-.*')
do
  jira=`sed 's/TINKERPOP3/TINKERPOP/' <<< $branch | grep -Po 'TINKERPOP-[0-9]*'`
  curl -s https://issues.apache.org/jira/browse/$jira > /tmp/$jira
  status=`cat /tmp/$jira | grep -A1 status-val | grep -Po '(?<=>)[^<]*(?=)' | head -n1`
  title=`cat /tmp/$jira | grep -Po '(?<=<title>).*(?=</title>)' | head -n1 | recode html..ascii | sed 's/ - ASF JIRA//'`
  if [ "$status" == "Closed" ]; then
    if [ "$1" == "--delete" ]; then
      git push origin --delete $branch
      git branch -D $branch
      git branch -r -D origin/$branch
    else
      echo "$branch -- $title"
    fi
  fi
  rm /tmp/$jira
done
