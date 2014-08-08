#!/bin/bash

git checkout publish-docs
git fetch origin
git merge origin/master
git push origin publish-docs
git checkout master
