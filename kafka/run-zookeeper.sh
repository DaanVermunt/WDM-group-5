#!/usr/bin/env bash
cd kafka_2.11-1.1.0

bin/zookeeper-server-stop.sh

bin/zookeeper-server-start.sh \
  config/zookeeper.properties
