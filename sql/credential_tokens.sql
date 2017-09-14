CREATE TABLE `credential_tokens` (
  `gamespace_id` int(11) NOT NULL,
  `account_id` int(11) DEFAULT NULL,
  `credential` varchar(255) NOT NULL,
  `username` varchar(255) NOT NULL,
  `access_token` mediumtext NOT NULL,
  `expires_at` datetime DEFAULT NULL,
  `payload` json NOT NULL,
  `merged_credential` varchar(512) NOT NULL DEFAULT '',
  UNIQUE KEY `credential_unique` (`gamespace_id`,`credential`,`username`),
  KEY `gamespace_id` (`gamespace_id`,`merged_credential`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;