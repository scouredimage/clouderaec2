# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import subprocess
import sys
import time
import logging

from hadoop.cloud.providers.ec2 import Ec2Cluster
from hadoop.cloud.cluster import TimeoutException

logger = logging.getLogger(__name__)

class Ec2SpotCluster(Ec2Cluster):
  """
  A cluster of EC2 SPOT instances. A cluster has a unique name.

  Instances running in the cluster run in a security group with the cluster's
  name, and also a name indicating the instance's role, e.g. <cluster-name>-foo
  to show a "foo" instance.
  """

  def __init__(self, name, config_dir):
    super(Ec2SpotCluster, self).__init__(name, config_dir)

  def get_provider_code(self):
    return "ec2-spot"

  def launch_instances(self, roles, number, image_id, size_id,
                       instance_user_data, **kwargs):
    for role in roles:
      self._check_role_name(role)
      self._create_groups(role)
      
    user_data = instance_user_data.read_as_gzip_stream()
    security_groups = self._get_group_names(roles) + kwargs.get('security_groups', [])

    reservations = self.ec2Connection.request_spot_instances(
        kwargs.get('price',None),
        image_id,
        count=number,
        user_data=user_data,
        instance_type=size_id,
        key_name=kwargs.get('key_name', None),
        security_groups=security_groups,
        placement=kwargs.get('placement', None),
        launch_group=kwargs.get('launch_group'))

    return reservations

  def wait_for_instances(self, reservations, roles=None, timeout=600, sleep=5):
    if roles is None:
      raise Exception("roles are required for Spot Instances")

    try:
      for retry in range(0, timeout, sleep):
        reservations = self.ec2Connection.get_all_spot_instance_requests([r.id for r in reservations])
        if self._all_started(reservations, roles):
          reservations = []
          break
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(sleep)
      else:
        instances = [i.id for i in self._get_instances_with_roles(roles)]
        if instances:
          self.ec2Connection.terminate_instances(instances)
        raise TimeoutException()
    finally:
      print
      map(lambda r: r.cancel(), reservations)

  def _all_started(self, reservations, roles):
    for reservation in reservations:
      if reservation.fault:
        raise Exception("Reservation Error: %s" % reservation.fault)
      if reservation.state != "active":
        return False

    instances = self._get_instances_with_roles(roles)
    reservation_ids = set([r.id for r in reservations])
    for i in instances:
      if i.state != "running" and i.spot_instance_request_id in reservation_ids:
        return False
    return len(instances) > 0

  def _get_instances_with_roles(self, roles):
    instances = {}
    for role in roles:
      for instance in self._get_instances(self._group_name_for_role(role)):
        instances[instance.id] = instance
    return instances.values()
