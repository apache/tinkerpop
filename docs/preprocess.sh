#!/bin/bash

pushd "$(dirname $0)/.." > /dev/null

#for doc in $(find docs/src/ -name "the-traversal.asciidoc")
for doc in $(find docs/src/ -name "*.asciidoc")
do
  name=`basename $doc`
  output="docs/${name}"
  echo "${doc} > ${output}"
  bin/gremlin.sh -e docs/preprocessor/processor.groovy $doc > $output
  # TODO: exit in case of an error doesn't work as expected yet
  ec=$?
  if [ $ec -ne 0 ]; then
    popd >/dev/null
    exit $ec
  fi
done

popd > /dev/null
