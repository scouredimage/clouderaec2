function FindProxyForURL(url, host) {
  if ((shExpMatch(host, "*ec2*.amazonaws.com*")) ||
      (shExpMatch(host, "*ec2.internal*")) ||
      (shExpMatch(host, "*domu*.internal*"))) {
    return "SOCKS localhost:6666";
  }
  return "DIRECT";
}

