CREATE TABLE `wsr_user_group_ref` (
  `user_group_id` int(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_group_type_id` int(20) NOT NULL,
  `insert_time` varchar(255) NOT NULL,
   FOREIGN KEY (user_group_type_id) REFERENCES wsr_user_group_type(user_group_type_id)
);