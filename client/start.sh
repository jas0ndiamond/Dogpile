#!/bin/bash

echo "Starting dispynode.py..."

#must run with --daemon or else docker will kill it

/home/dispy/venv_dispy/bin/python3 /home/dispy/venv_dispy/bin/dispynode.py\
 -d\
 --cpus $(/bin/cat /proc/cpuinfo | /bin/grep processor | /usr/bin/wc -l)\
 --dest_path_prefix /home/dispy/node_tmp\
 --name $DISPY_NODE_NAME\
 -i $(/bin/hostname -I)\
 --clean\
 --msg_timeout 12000\
 -s $DISPY_SECRET\
 --ext_host $HOST_IP\
 --daemon

sleep infinity
