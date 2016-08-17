CREATE TABLE `credential_tokens` (
  `gamespace_id` int(11) NOT NULL,
  `account_id` int(11) DEFAULT NULL,
  `credential` varchar(255) NOT NULL,
  `username` varchar(255) NOT NULL,
  `access_token` mediumtext NOT NULL,
  `expires_at` datetime NOT NULL,
  `payload` json NOT NULL,
  PRIMARY KEY (`gamespace_id`,`credential`,`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;