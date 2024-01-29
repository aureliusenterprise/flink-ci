#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cp -avf "${SCRIPT_DIR}/files/*" /opt/apache-atlas-2.2.0/

export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

touch /opt/apache-atlas-2.2.0/logs/application.log

cd /opt/apache-atlas-2.2.0/
./bin/atlas_start.py

tail -f /dev/null
