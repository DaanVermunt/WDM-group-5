#!/usr/bin/env bash
cd kafka_2.11-1.1.0

## Create topics
bin/kafka-topics.sh --create \
    --replication-factor 1 \
    --partitions 1 \
    --topic Listings \
    --zookeeper  localhost:2181

    bin/kafka-topics.sh --create \
    --replication-factor 1 \
    --partitions 1 \
    --topic Calendar \
    --zookeeper  localhost:2181
    
    bin/kafka-topics.sh --create \
    --replication-factor 1 \
    --partitions 1 \
    --topic Review \
    --zookeeper  localhost:2181
