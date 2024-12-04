#!/usr/bin/env bash

cluster_name='demo'
seed_address='192.168.0.163'
listen_address="$(hostname --all-ip-addresses)"
rpc_address="$(hostname --all-ip-addresses)"

# Download tarball.
curl --remote-name https://dlcdn.apache.org/cassandra/5.0.2/apache-cassandra-5.0.2-bin.tar.gz

# Extract files from tarball.
tar -xvf apache-cassandra-5.0.2-bin.tar.gz

# Backup configuration file for comparison later.
cp apache-cassandra-5.0.2/conf/cassandra.yaml apache-cassandra-5.0.2/conf/cassandra.yaml.original

# Set cluster_name.
sed -i "s/cluster_name: 'Test Cluster'/cluster_name: $cluster_name/g" apache-cassandra-5.0.2/conf/cassandra.yaml

# Set seeds.
sed -i "s/- seeds: \"127.0.0.1:7000\"/- seeds: $seed_address:7000/g" apache-cassandra-5.0.2/conf/cassandra.yaml

# Set listen_address.
sed -i "s/listen_address: localhost/listen_address: $listen_address/g" apache-cassandra-5.0.2/conf/cassandra.yaml

# Set rpc_address.
sed -i "s/rpc_address: localhost/rpc_address: $rpc_address/g" apache-cassandra-5.0.2/conf/cassandra.yaml
