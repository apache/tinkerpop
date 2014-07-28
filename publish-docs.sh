#!/bin/bash

git checkout publish-docs
git pull
git merge origin/master
git push origin publish-docs
git checkout master
