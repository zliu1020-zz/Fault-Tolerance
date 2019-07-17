#!/bin/bash

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

KV_PORT=10274
echo Port number: $KV_PORT

$JAVA_HOME/bin/java StorageNode `hostname` $KV_PORT $ZKSTRING /$USER
