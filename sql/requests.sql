CREATE TABLE `requests` (
  `account_id` int(11) NOT NULL,
  `gamespace_id` int(11) NOT NULL,
  `request_type` enum('account','group') NOT NULL DEFAULT 'account',
  `request_object` int(11) NOT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `request_expire` datetime NOT NULL,
  `request_key` varchar(96) NOT NULL DEFAULT '',
  `request_payload` json DEFAULT NULL,
  UNIQUE KEY `account_id` (`account_id`,`gamespace_id`,`request_type`,`request_object`),
  KEY `account_id_2` (`account_id`),
  KEY `request_key` (`request_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;