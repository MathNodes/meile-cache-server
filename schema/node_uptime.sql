CREATE TABLE `node_uptime` (
  `node_address` varchar(100) NOT NULL,
  `remote_url` varchar(500) DEFAULT NULL,
  `tries` bigint NOT NULL DEFAULT '0',
  `success` bigint NOT NULL DEFAULT '0',
  `success_rate` decimal(6,3) NOT NULL DEFAULT '0.000',
  PRIMARY KEY (`node_address`)
