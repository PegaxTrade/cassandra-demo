#!/usr/bin/env bash

# Guard against unintentional execution of this script.
exit 0

# ---
# The following commands should be run in the install location, e.g. ~/apache-cassandra-5.0.2/
# ---

# Start Cassandra daemon on this node.
# - This command exit immediately even though the stdout keeps printing logs.
# - A Cassandra node should be started after the previous node has joined the cluster successfully (i.e. nodetool
#   reports the status as "Normal" instead of "Joining").
./bin/cassandra

# Stop incoming connections on this node.
./bin/nodetool drain

# Stop Cassandra daemon on this node.
./bin/nodetool stopdaemon

# Check Cassandra cluster status.
./bin/nodetool status

# Check Cassandra system log.
less ./logs/system.log

# ---
# Misc
# ---

# Check if there is any Cassandra process.
ps aux | grep cassandra

# Check if Cassandra binds to the correct ports.
netstat -nltp
netstat --numeric --listening --tcp --programs

# Clean up Cassandra installation, tarball and temporary files.
rm -r apache-cassandra-5.0.2 apache-cassandra-5.0.2-bin.tar.gz .cassandra
