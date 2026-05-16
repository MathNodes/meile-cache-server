CREATE TABLE `nodes_ip` (
  `node_address` varchar(100) NOT NULL,
  `asn` varchar(15) DEFAULT NULL,
  `isp` varchar(200) DEFAULT NULL,
  `isp_type` varchar(50) DEFAULT NULL,
  `datacenter` tinyint(1) DEFAULT NULL,
  `last_checked` timestamp NULL DEFAULT NULL,
  `ip` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`node_address`),
  KEY `idx_ip` (`ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;