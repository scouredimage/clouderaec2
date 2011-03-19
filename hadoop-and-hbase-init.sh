#!/bin/bash -ex
export %ENV%
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

REPO="cdh3b3"
HADOOP="hadoop-0.20"

update_repo() {
  add-apt-repository "deb http://archive.canonical.com/ lucid partner"
  cat > /etc/apt/sources.list.d/cloudera.list <<EOF
deb http://archive.cloudera.com/debian lucid-$REPO contrib
deb-src http://archive.cloudera.com/debian lucid-$REPO contrib
EOF
  curl -s http://archive.cloudera.com/debian/archive.key | apt-key add -
  apt-get update
  cat > /tmp/debconf-jre-selections <<EOF
sun-java5-jdk shared/accepted-sun-dlj-v1-1 select true
sun-java5-jre shared/accepted-sun-dlj-v1-1 select true
sun-java6-jdk shared/accepted-sun-dlj-v1-1 select true
sun-java6-jre shared/accepted-sun-dlj-v1-1 select true
EOF
  /usr/bin/debconf-set-selections /tmp/debconf-jre-selections
}

install_user_packages() {
  if [ ! -z "$USER_PACKAGES" ]; then
    apt-get -y install $USER_PACKAGES
  fi
}

install_hadoop() {
  apt-get -y install $HADOOP
  cp -r /etc/$HADOOP/conf.empty /etc/$HADOOP/conf.dist
  update-alternatives --install /etc/$HADOOP/conf $HADOOP-conf /etc/$HADOOP/conf.dist 90
  apt-get -y install hadoop-pig${PIG_VERSION:+-${PIG_VERSION}}
  apt-get -y install hadoop-hive${HIVE_VERSION:+-${HIVE_VERSION}}
  apt-get -y install policykit 
}

prep_disk() {
  mount=$1
  device=$2
  automount=${3:-false}

  echo "warning: ERASING CONTENTS OF $device"
  mkfs.xfs -f $device
  if [ ! -e $mount ]; then
    mkdir $mount
  fi
  mount -o defaults,noatime $device $mount
  if $automount ; then
    echo "$device $mount xfs defaults,noatime 0 0" >> /etc/fstab
  fi
}

wait_for_mount() {
  mount=$1
  device=$2

  mkdir $mount

  i=1
  echo "Attempting to mount $device"
  while true ; do
    sleep 10
    echo -n "$i "
    i=$[$i+1]
    umount -f $mount
    mount -o defaults,noatime $device $mount || continue
    echo " Mounted."
    break;
  done
}

make_hadoop_dirs() {
  for mount in "$@"; do
    if [ ! -e $mount/hadoop ]; then
      mkdir -p $mount/hadoop
      mkdir -p $mount/hadoop/mapred/local
    fi
    chown hdfs:hadoop $mount/hadoop
    chown -R mapred:hadoop $mount/hadoop/mapred
  done
}

configure_hadoop() {

  apt-get -y install xfsprogs # needed for XFS

  INSTANCE_TYPE=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-type`

  if [ -n "$EBS_MAPPINGS" ]; then
    # EBS_MAPPINGS is like "/ebs1,/dev/sdj;/ebs2,/dev/sdk"
    DFS_NAME_DIR=''
    FS_CHECKPOINT_DIR=''
    DFS_DATA_DIR=''
    for mapping in $(echo "$EBS_MAPPINGS" | tr ";" "\n"); do
      # Split on the comma (see "Parameter Expansion" in the bash man page)
      mount=${mapping%,*}
      device=${mapping#*,}
      wait_for_mount $mount $device
      DFS_NAME_DIR=${DFS_NAME_DIR},"$mount/hadoop/hdfs/name"
      FS_CHECKPOINT_DIR=${FS_CHECKPOINT_DIR},"$mount/hadoop/hdfs/secondary"
      DFS_DATA_DIR=${DFS_DATA_DIR},"$mount/hadoop/hdfs/data"
      FIRST_MOUNT=${FIRST_MOUNT-$mount}
      make_hadoop_dirs $mount
    done
    # Remove leading commas
    DFS_NAME_DIR=${DFS_NAME_DIR#?}
    FS_CHECKPOINT_DIR=${FS_CHECKPOINT_DIR#?}
    DFS_DATA_DIR=${DFS_DATA_DIR#?}

    DFS_REPLICATION=3 # EBS is internally replicated, but we also use HDFS replication for safety
  else
    case $INSTANCE_TYPE in
    m1.xlarge|c1.xlarge)
      DFS_NAME_DIR=/mnt/hadoop/hdfs/name,/mnt2/hadoop/hdfs/name
      FS_CHECKPOINT_DIR=/mnt/hadoop/hdfs/secondary,/mnt2/hadoop/hdfs/secondary
      DFS_DATA_DIR=/mnt/hadoop/hdfs/data,/mnt2/hadoop/hdfs/data,/mnt3/hadoop/hdfs/data,/mnt4/hadoop/hdfs/data
      ;;
    *)
      DFS_NAME_DIR=/mnt/hadoop/hdfs/name
      FS_CHECKPOINT_DIR=/mnt/hadoop/hdfs/secondary
      DFS_DATA_DIR=/mnt/hadoop/hdfs/data
      ;;
    esac
    FIRST_MOUNT=/mnt
    DFS_REPLICATION=3
  fi

  case $INSTANCE_TYPE in
  c1.xlarge)
    prep_disk /mnt2 /dev/sdc true &
    disk2_pid=$!
    prep_disk /mnt3 /dev/sdd true &
    disk3_pid=$!
    prep_disk /mnt4 /dev/sde true &
    disk4_pid=$!
    wait $disk2_pid $disk3_pid $disk4_pid
    MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local,/mnt2/hadoop/mapred/local,/mnt3/hadoop/mapred/local,/mnt4/hadoop/mapred/local
    MAX_MAP_TASKS=3
    MAX_REDUCE_TASKS=2
    CHILD_OPTS=-Xmx1024m
    CHILD_ULIMIT=2097152
    HADOOP_NAMENODE_OPTS=${HADOOP_JOBTRACKER_OPTS:='-Xmx4096m -XX:+UseParallelGC'}
    HADOOP_HEAPSIZE='2048'
    ;;
  m1.xlarge)
    prep_disk /mnt2 /dev/sdc true &
    disk2_pid=$!
    prep_disk /mnt3 /dev/sdd true &
    disk3_pid=$!
    prep_disk /mnt4 /dev/sde true &
    disk4_pid=$!
    wait $disk2_pid $disk3_pid $disk4_pid
    MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local,/mnt2/hadoop/mapred/local,/mnt3/hadoop/mapred/local,/mnt4/hadoop/mapred/local
    MAX_MAP_TASKS=7
    MAX_REDUCE_TASKS=3
    CHILD_OPTS=-Xmx1024m
    CHILD_ULIMIT=2097152
    HADOOP_NAMENODE_OPTS=${HADOOP_JOBTRACKER_OPTS:='-Xmx4096m -XX:+UseParallelGC'}
    HADOOP_HEAPSIZE='2048'
    ;;
  *)
    MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local
    MAX_MAP_TASKS=2
    MAX_REDUCE_TASKS=1
    CHILD_OPTS=-Xmx550m
    CHILD_ULIMIT=1126400
    ;;
  esac

  make_hadoop_dirs `ls -d /mnt*`

  # Create tmp directory
  mkdir /mnt/tmp
  chmod a+rwxt /mnt/tmp

  cat > /etc/$HADOOP/conf.dist/hadoop-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>dfs.block.size</name>
  <value>134217728</value>
  <final>true</final>
</property>
<property>
  <name>dfs.data.dir</name>
  <value>$DFS_DATA_DIR</value>
  <final>true</final>
</property>
<property>
  <name>dfs.datanode.du.reserved</name>
  <value>1073741824</value>
  <final>true</final>
</property>
<property>
  <name>dfs.datanode.handler.count</name>
  <value>3</value>
  <final>true</final>
</property>
<!--property>
  <name>dfs.hosts</name>
  <value>/etc/$HADOOP/conf.dist/dfs.hosts</value>
  <final>true</final>
</property-->
<!--property>
  <name>dfs.hosts.exclude</name>
  <value>/etc/$HADOOP/conf.dist/dfs.hosts.exclude</value>
  <final>true</final>
</property-->
<property>
  <name>dfs.name.dir</name>
  <value>$DFS_NAME_DIR</value>
  <final>true</final>
</property>
<property>
  <name>dfs.namenode.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>dfs.permissions</name>
  <value>true</value>
  <final>true</final>
</property>
<property>
  <name>dfs.replication</name>
  <value>$DFS_REPLICATION</value>
</property>
<property>
  <name>fs.checkpoint.dir</name>
  <value>$FS_CHECKPOINT_DIR</value>
  <final>true</final>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://$NN_HOST:8020/</value>
</property>
<property>
  <name>fs.trash.interval</name>
  <value>1440</value>
  <final>true</final>
</property>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/tmp/hadoop-\${user.name}</value>
  <final>true</final>
</property>
<property>
  <name>io.file.buffer.size</name>
  <value>65536</value>
</property>
<property>
  <name>mapred.child.java.opts</name>
  <value>$CHILD_OPTS</value>
</property>
<property>
  <name>mapred.child.ulimit</name>
  <value>$CHILD_ULIMIT</value>
  <final>true</final>
</property>
<property>
  <name>mapred.job.tracker</name>
  <value>$JT_HOST:8021</value>
</property>
<property>
  <name>mapred.job.tracker.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>mapred.local.dir</name>
  <value>$MAPRED_LOCAL_DIR</value>
  <final>true</final>
</property>
<property>
  <name>mapred.map.tasks.speculative.execution</name>
  <value>true</value>
</property>
<property>
  <name>mapred.reduce.parallel.copies</name>
  <value>10</value>
</property>
<property>
  <name>mapred.reduce.tasks</name>
  <value>10</value>
</property>
<property>
  <name>mapred.reduce.tasks.speculative.execution</name>
  <value>false</value>
</property>
<property>
  <name>mapred.submit.replication</name>
  <value>10</value>
</property>
<property>
  <name>mapred.system.dir</name>
  <value>/mapred/system</value>
</property>
<property>
  <name>mapreduce.jobtracker.staging.root.dir</name>
  <value>/user</value>
</property>
<property>
  <name>mapred.tasktracker.map.tasks.maximum</name>
  <value>$MAX_MAP_TASKS</value>
  <final>true</final>
</property>
<property>
  <name>mapred.tasktracker.reduce.tasks.maximum</name>
  <value>$MAX_REDUCE_TASKS</value>
  <final>true</final>
</property>
<property>
  <name>tasktracker.http.threads</name>
  <value>46</value>
  <final>true</final>
</property>
<property>
  <name>mapred.jobtracker.taskScheduler</name>
  <value>org.apache.hadoop.mapred.FairScheduler</value>
</property>
<property>
  <name>mapred.fairscheduler.allocation.file</name>
  <value>/etc/$HADOOP/conf.dist/fairscheduler.xml</value>
</property>
<property>
  <name>mapred.compress.map.output</name>
  <value>true</value>
</property>
<property>
  <name>mapred.output.compression.type</name>
  <value>BLOCK</value>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.default</name>
  <value>org.apache.hadoop.net.StandardSocketFactory</value>
  <final>true</final>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.ClientProtocol</name>
  <value></value>
  <final>true</final>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.JobSubmissionProtocol</name>
  <value></value>
  <final>true</final>
</property>
<property>
  <name>io.compression.codecs</name>
  <value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec</value>
</property>
<property>
  <name>fs.s3.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>
<property>
  <name>fs.s3.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>
<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>
<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>
<property>
  <name>dfs.support.append</name>
  <value>true</value>
</property>
<property>
  <name>dfs.datanode.max.xcievers</name>
  <value>4096</value>
</property>
</configuration>
EOF

  cat > /etc/$HADOOP/conf.dist/fairscheduler.xml <<EOF
<?xml version="1.0"?>
<allocations>
</allocations>
EOF

  cat > /etc/$HADOOP/conf.dist/hadoop-metrics.properties <<EOF
dfs.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
mapred.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
jvm.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
rpc.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
EOF

  HADOOP_ENV="/etc/$HADOOP/conf.dist/hadoop-env.sh"

  # Keep PID files in a non-temporary directory
  sed -i -e "s|# export HADOOP_PID_DIR=.*|export HADOOP_PID_DIR=/var/run/hadoop|" $HADOOP_ENV
  mkdir -p /var/run/hadoop
  chown -R mapred:hadoop /var/run/hadoop

  # Set SSH options within the cluster
  sed -i -e 's|# export HADOOP_SSH_OPTS=.*|export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no"|' $HADOOP_ENV

  # Bump up VM limits
  sed -i -e "s|export HADOOP_NAMENODE_OPTS=\"\(.*\)\"|export HADOOP_NAMENODE_OPTS=\"$HADOOP_NAMENODE_OPTS \1\"|" $HADOOP_ENV
  sed -i -e "s|export HADOOP_JOBTRACKER_OPTS=\"\(.*\)\"|export HADOOP_JOBTRACKER_OPTS=\"$HADOOP_JOBTRACKER_OPTS \1\"|" $HADOOP_ENV
  sed -i -e "s|# export HADOOP_HEAPSIZE=.*|export HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE-1000}|" $HADOOP_ENV

  # Hadoop logs should be on the /mnt partition
  rm -rf /var/log/hadoop-0.20
  mkdir -p /mnt/hadoop-0.20/logs
  chown mapred:hadoop /mnt/hadoop-0.20/logs
  chmod g+rwxt /mnt/hadoop-0.20/logs
  ln -s /mnt/hadoop-0.20/logs /var/log/hadoop-0.20
  chown -R mapred:hadoop /var/log/hadoop-0.20
  chmod g+rwxt /var/log/hadoop-0.20
}

configure_hbase() {
  cat > /etc/hbase/conf/hbase-site.xml <<EOF
<configuration>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://$NN_HOST:8020/hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>$NN_HOST</value>
  </property>
  <property>
    <name>dfs.support.append</name>
    <value>true</value>
  </property>
</configuration>
EOF

  rm -rf /var/log/hbase
  mkdir -p /mnt/hbase/logs
  chown hbase:hbase /mnt/hbase/logs
  ln -s /mnt/hbase/logs /var/log/hbase
  chown -R hbase:hbase /var/log/hbase
}

setup_zookeeper() {
  apt-get -y install hadoop-zookeeper-server

  ps -aef | grep QuorumPeer | grep zookeeper | awk {'print $2'} | xargs kill 
  perl -pi -e "s/localhost/$NN_HOST/" /etc/zookeeper/zoo.cfg

  rm -rf /var/log/zookeeper
  mkdir -p /mnt/zookeeper/logs
  chown zookeeper:zookeeper /mnt/zookeeper/logs
  ln -s /mnt/zookeeper/logs /var/log/zookeeper
  chown -R zookeeper:zookeeper /var/log/zookeeper

  service hadoop-zookeeper-server start
}

setup_web() {
  apt-get -y install thttpd
  WWW_BASE=/var/www

  cat > $WWW_BASE/index.html << END
<html>
<head>
<title>Hadoop EC2 Cluster</title>
</head>
<body>
<h1>Hadoop EC2 Cluster</h1>
<ul>
<li><a href="http://$NN_HOST:50070/">NameNode</a>
<li><a href="http://$JT_HOST:50030/">JobTracker</a>
</ul>
</body>
</html>
END
  service thttpd start
}

start_daemon() {
  apt-get -y install $HADOOP-$1
  service $HADOOP-$1 start
}

start_hbase_daemon() {
  apt-get -y install $1
  configure_hbase
  /etc/init.d/$1 start
}

start_namenode() {
  AS_HDFS="su -s /bin/bash - hdfs -c"
  [ ! -e $FIRST_MOUNT/hadoop/hdfs ] && $AS_HDFS "/usr/bin/$HADOOP namenode -format"

  start_daemon namenode

  $AS_HDFS "$HADOOP dfsadmin -safemode wait"

  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /user"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod a+w /user"

  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /tmp"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod a+w /tmp"

  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /user/hive/warehouse"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /user/hive/warehouse"

  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /hbase"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /hbase"
  $AS_HDFS "/usr/bin/$HADOOP fs -chown hbase:hbase /hbase"

  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /hadoop"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /hadoop"
  $AS_HDFS "/usr/bin/$HADOOP fs -chown mapred:hadoop /hadoop"

  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /mapred/system"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod 700 /mapred/system"
  $AS_HDFS "/usr/bin/$HADOOP fs -chown -R mapred:hadoop /mapred"
}

SELF_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-ipv4`
for role in $(echo "$ROLES" | tr "," "\n"); do
  case $role in
  nn)
    NN_HOST=$SELF_HOST
    ;;
  jt)
    JT_HOST=$SELF_HOST
    ;;
  esac
done

update_repo
install_user_packages
install_hadoop
configure_hadoop

for role in $(echo "$ROLES" | tr "," "\n"); do
  case $role in
  nn)
    setup_web
    start_namenode
#    setup_zookeeper
#    start_hbase_daemon hadoop-hbase-master
    ;;
  snn)
    start_daemon secondarynamenode
    ;;
  jt)
    start_daemon jobtracker
    ;;
  dn)
    start_daemon datanode
#    start_hbase_daemon hadoop-hbase-regionserver
    ;;
  tt)
    start_daemon tasktracker
    ;;
  esac
done
