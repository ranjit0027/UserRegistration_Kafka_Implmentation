CREATE TABLE `wsr_user_address` (
  `user_address_id` int(11) NOT NULL AUTO_INCREMENT,
  `userid` int(11) NOT NULL,
  `addresstype` varchar(20) DEFAULT NULL,
  `addressline1` varchar(255) NOT NULL,
  `addressline2` varchar(255) NOT NULL,
  `phone` varchar(20) NOT NULL,
  `city` varchar(100) NOT NULL,
  `state` varchar(100) NOT NULL,
  `country` varchar(20) NOT NULL,
  `postalcode` varchar(20) NOT NULL,
  `email` varchar(20) NOT NULL,
  `activeflag` tinyint(1) DEFAULT '0',
  `createdate` varchar(255) NOT NULL,
  `updatedate` varchar(255) NOT NULL,
  `inactivedate` varchar(255) NOT NULL,
   PRIMARY KEY (user_address_id),
   FOREIGN KEY (userid) REFERENCES wsr_user_account(userid)
);
