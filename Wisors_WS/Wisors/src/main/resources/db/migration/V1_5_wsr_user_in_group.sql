CREATE TABLE `wsr_user_in_group` (
  `user_in_group_id` int(11) NOT NULL AUTO_INCREMENT,
  `delete_time` varchar(255) DEFAULT NULL,
  `group_admin` tinyint(1) DEFAULT 0,
  `insert_time` varchar(255) DEFAULT NULL,
  `userid` int(11) NOT NULL,
  `user_group_id` int(11) NOT NULL,
   PRIMARY KEY (user_in_group_id),
   FOREIGN KEY (userid) REFERENCES wsr_user_account(userid),
   FOREIGN KEY (user_group_id) REFERENCES wsr_user_group_ref(user_group_id)
 
);
