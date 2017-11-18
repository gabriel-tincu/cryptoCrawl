#!/bin/bash
here=`pwd`
parent=`dirname $here`
export GOPATH=`dirname $parent`
echo $GOPATH
for f in `cat requirements`
do
  go get $f
done
