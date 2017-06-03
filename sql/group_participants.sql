CREATE TABLE `group_participants` (
  `group_id` int(11) unsigned NOT NULL,
  `gamespace_id` int(11) NOT NULL,
  `account_id` int(11) NOT NULL,
  `participation_role` int(11) NOT NULL,
  `participation_permissions` varchar(255) NOT NULL,
  `participation_profile` json NOT NULL,
  UNIQUE KEY `group_id` (`group_id`,`gamespace_id`,`account_id`),
  KEY `account_id` (`account_id`),
  CONSTRAINT `group_participants_ibfk_1` FOREIGN KEY (`group_id`) REFERENCES `groups` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;