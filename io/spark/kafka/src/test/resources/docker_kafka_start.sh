#!/bin/bash

sleep 4

# Fix kafka config
sed -i 's/$HOSTNAME/172.26.0.9/g' /opt/kafka/config/server.properties

# Start kafka
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties