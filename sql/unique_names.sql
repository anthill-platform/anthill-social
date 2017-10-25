CREATE TABLE `unique_names` (
  `gamespace_id` int(11) unsigned NOT NULL,
  `account_id` int(11) unsigned NOT NULL,
  `kind` varchar(255) NOT NULL DEFAULT '',
  `name` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`gamespace_id`,`account_id`,`kind`),
  UNIQUE KEY `gamespace_id` (`gamespace_id`,`kind`,`name`),
  KEY `account_id` (`account_id`,`name`),
  FULLTEXT KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
