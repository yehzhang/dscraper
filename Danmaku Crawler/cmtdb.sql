-- CREATE TABLE `avmetadata` (
--   `av_id` int(10) unsigned NOT NULL COMMENT 'Aid',
--   PRIMARY KEY (`av_id`)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `chatmetadata` (
  `chat_id` int(10) unsigned NOT NULL COMMENT '主键',
  `chat_source` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'unknown',
  `chat_max_limit` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '最大弹幕数',
  `chat_max_count` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'unknown',
  `chat_mission` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'unknown',
  `chat_crawled_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `av_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Aid',
  PRIMARY KEY (`chat_id`)-- , KEY `chat_foreign_aid_idx` (`av_id`), CONSTRAINT `chat_foreign_aid` FOREIGN KEY (`av_id`) REFERENCES `avmetadata` (`av_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `comments` (
  `cmt_id` bigint(10) unsigned NOT NULL COMMENT '在弹幕数据库中的Row ID，用于“历史弹幕”功能，有重复',
  `cmt_time` float NOT NULL COMMENT '出现的时间，单位为秒',
  `cmt_mode` tinyint(1) NOT NULL DEFAULT '1' COMMENT '模式\n1-3: 滚动弹幕\n4: 底端弹幕\n5: 顶端弹幕\n6: 逆向弹幕\n7: 精准定位',
  `cmt_size` tinyint(3) unsigned NOT NULL DEFAULT '25' COMMENT '字号\n12: 非常小\n16: 特小\n18: 小\n25: 中\n36: 大\n45: 很大\n64: 特别大',
  `cmt_color` mediumint(8) unsigned NOT NULL DEFAULT '16777215' COMMENT '颜色，HTML颜色的十进制数',
  `cmt_date` timestamp NOT NULL DEFAULT '1970-01-01 08:00:01' COMMENT '时间戳，mysql格式',
  `cmt_pool` tinyint(2) NOT NULL DEFAULT '0' COMMENT '弹幕池',
  `cmt_user_id` varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '发送者的ID，等同于用php crc32($UID)生成的32bit数。如果以大写D开头则为游客',
  `cmt_content` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '内容',
  `chat_id` int(10) unsigned NOT NULL COMMENT '所属的Cid',
  PRIMARY KEY (`cmt_id`,`cmt_date`),
  KEY `cmt_foreign_cid_idx` (`chat_id`),
  CONSTRAINT `cmt_foreign_cid` FOREIGN KEY (`chat_id`) REFERENCES `chatmetadata` (`chat_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `specialcomments` (
  `cmt_id` bigint(10) unsigned NOT NULL COMMENT '在弹幕数据库中的Row ID，用于“历史弹幕”功能，有重复',
  `cmt_date` timestamp NOT NULL DEFAULT '1970-01-01 08:00:01' COMMENT '时间戳，mysql格式',
  `cmt_user_id` varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '发送者的ID，等同于用php crc32($UID)生成的32bit数。如果以大写D开头则为游客',
  `cmt_content` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '内容',
  `chat_id` int(10) unsigned NOT NULL COMMENT '所属的Cid',
  PRIMARY KEY (`cmt_id`,`cmt_date`),
  KEY `spcmt_foreign_cid_idx` (`chat_id`),
  CONSTRAINT `spcmt_foreign_cid` FOREIGN KEY (`chat_id`) REFERENCES `chatmetadata` (`chat_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

