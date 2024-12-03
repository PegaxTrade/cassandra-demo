#!/usr/bin/env bash

cluster_name='demo'
seed_address='192.168.0.163'
listen_address="$(hostname --all-ip-addresses)"

# Download tarball.
curl --remote-name https://dlcdn.apache.org/cassandra/5.0.2/apache-cassandra-5.0.2-bin.tar.gz

# Extract files from tarball.
tar -xvf apache-cassandra-5.0.2-bin.tar.gz

# Set cluster name.
sed -i "s/cluster_name: 'Test Cluster'/cluster_name: $cluster_name/g" apache-cassandra-5.0.2/conf/cassandra.yaml

# Set seed node.
sed -i "s/- seeds: \"127.0.0.1:7000\"/- seeds: seed_address:7000/g" apache-cassandra-5.0.2/conf/cassandra.yaml

# Set listen address.
sed -i "s/listen_address: localhost/listen_address: $listen_address/g" apache-cassandra-5.0.2/conf/cassandra.yaml
