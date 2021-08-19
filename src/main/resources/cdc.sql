CREATE TABLE `pv` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pageId` varchar(256) NOT NULL,
  `userId` varchar(128) NOT NULL,
  `startTime` datetime NOT NULL,
  `endTime` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_pageId` (`pageId`),
  KEY `idx_userId` (`userId`),
  KEY `idx_startTime` (`startTime`)
)