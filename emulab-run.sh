#!/bin/bash

cat /etc/hosts | grep node | awk '{print $4}' > /tmp/hosts

parallel-ssh -i -h /tmp/hosts -I sudo bash <<EOF

# Copy ssh keys and public keys so that root user can freely login to other nodes without prompted for password
if [ "`sudo cat /root/.ssh/authorized_keys | wc -l`" -lt "10" ]; then
cp /users/yossupp/.ssh/id_rsa /root/.ssh/
cat /users/yossupp/.ssh/authorized_keys >> /root/.ssh/authorized_keys
fi


# Adjust max number of files
if [ "\`ulimit -n\`" -eq "1024" ]; then
cat << FOE >> /etc/security/limits.conf

*    -   memlock  unlimited
*    -   nofile   100000
root    -   memlock  unlimited
root    -   nofile   100000

FOE
fi


EOF


sudo -u root bash <<EOF

# Execute coordinator with emulab profile
python coordinator.py emulab 2>&1 | tee /tmp/morphus-cassandra-log.txt

EOF