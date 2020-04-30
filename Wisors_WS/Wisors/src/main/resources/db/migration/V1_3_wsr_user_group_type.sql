CREATE TABLE `wsr_user_group_type` (
  `user_group_type_id` int(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `group_name` varchar(20) NOT NULL,
  `userid` int(11) NOT NULL,
   FOREIGN KEY (userid) REFERENCES wsr_user_account(userid)
);