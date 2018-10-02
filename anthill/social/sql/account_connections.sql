CREATE TABLE `account_connections` (
  `account_id` int(11) unsigned NOT NULL,
  `account_connection` int(11) unsigned NOT NULL,
  UNIQUE KEY `account_id` (`account_id`,`account_connection`),
  KEY `account_id_2` (`account_id`),
  KEY `account_connection` (`account_connection`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;