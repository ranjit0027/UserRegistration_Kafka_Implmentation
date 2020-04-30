CREATE TABLE `wsr_user_account` (
  `userid` int(11) NOT NULL AUTO_INCREMENT,
  `usertype` varchar(20) NOT NULL,
  `firstname` varchar(255) NOT NULL,
  `lastname` varchar(255) NOT NULL,
  `phone` varchar(20) NOT NULL,
  `email` varchar(50) NOT NULL,
  `password` varchar(100) NOT NULL,  
  `activeflag` tinyint(1) DEFAULT '0',
  `createdate` varchar(255) NOT NULL,
  `updatedate` varchar(255) NOT NULL,
  `inactivedate` varchar(255) NOT NULL,
  `dob` varchar(255) NOT NULL,
  `gender` varchar(10) NOT NULL,
   PRIMARY KEY (userid)
);