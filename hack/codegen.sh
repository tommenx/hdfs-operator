#!/bin/bash -e

cd $GOPATH/src/k8s.io/code-generator && ./generate-groups.sh all \
  github.com/tommenx/hdfs-operator/pkg/client \
  github.com/tommenx/hdfs-operator/pkg/apis \
  storage.io:v1alpha1