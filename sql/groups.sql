CREATE TABLE `groups` (
  `group_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) NOT NULL,
  `group_name` varchar(255) DEFAULT NULL,
  `group_profile` json NOT NULL,
  `group_flags` set('messages') NOT NULL DEFAULT '',
  `group_free_members` int(11) unsigned NOT NULL DEFAULT '50',
  `group_join_method` enum('free','invite','approve') NOT NULL DEFAULT 'free',
  `group_owner` int(11) NOT NULL,
  PRIMARY KEY (`group_id`),
  FULLTEXT KEY `group_name` (`group_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;