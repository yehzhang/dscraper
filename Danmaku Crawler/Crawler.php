<?php

define('SERVER_ADDRESS', '127.0.0.1');
define('USER_NAME', 'root');
define('PASSWORD', '123456');
define('DBNAME', 'cmtdb');

define('PHP_INT_MIN', -PHP_INT_MAX - 1);

class Cralwer
{
	const CMT_TIME = 0;
	const CMT_MODE = 1;
	const CMT_SIZE = 2;
	const CMT_COLOR = 3;
	const CMT_DATE = 4;
	const CMT_POOL = 5;
	const CMT_USER_ID = 6;
	const CMT_ID = 7;
	const CMTENT_ATTRIS = 0;
	const CMTENT_CONTENT = 1;

	const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12';

	const MAX_REQUEST_TIMES = 5;
	const MAX_CIDS_SKIPPED = 10;
	const RELAX_DURATION = 1;

	private $start = 0;
	private $end = -1;
	private $fp;
	private $skippedCids;
	private $ch;
	private $cid = 'uninitialized';
	private $conn;
	private $hasFailed = false;
	private $startTime;

	private $hasLoggedEnd = false;

	function __construct($argc = 0, $argv = null)
	{
		if($argv === null)
		{
			$argc = 1;
		}

		date_default_timezone_set("Asia/Shanghai");

		$logName = '';
		switch($argc)
		{
			case 1:
				$logName .= 'default';
				break;
			case 2:
				$this->start = $argv[1];
				$this->end = $argv[1];
				$logName .= $argv[1];
				$this->cid = $this->start;
				break;
			case 3:
				$this->start = $argv[1];
				$this->end = $argv[2];
				$logName .= $argv[1].'_'.$argv[2];
				$this->cid = $this->start;
				break;
			default:
				die('inappropriate parameter for crawler class');
				break;
		}

		$logDir = 'log';
		if(!file_exists($logDir))
		{
			mkdir($logDir);
		}
		$logDir .= '/'.date("m-d");
		if(!file_exists($logDir))
		{
			mkdir($logDir);
		}
		$identifier = 1;
		$tmpLogName = '';
		do
		{
			$tmpLogName = $logDir.'/log_'.$logName.'_'.$identifier.'.ccl';
			$identifier++;
		}
		while(file_exists($tmpLogName));
		$logName = $tmpLogName;
		$this->fp = fopen($logName, "w");
		if($this->fp === false)
		{
			die("failed to create the log file\n");
		}
		$this->skippedCids = array();

		libxml_use_internal_errors(true);

		$this->ch = curl_init();
		curl_setopt($this->ch, CURLOPT_ENCODING, '');
		curl_setopt($this->ch, CURLOPT_RETURNTRANSFER, 1);
		curl_setopt($this->ch, CURLOPT_CONNECTTIMEOUT, 3);
		curl_setopt($this->ch, CURLOPT_TIMEOUT, 45);
		curl_setopt($this->ch, CURLOPT_REFERER, "http://www.baidu.com/");
		// curl_setopt($this->ch, CURLOPT_USERAGENT, self::UA);

		$this->connectToDatabase() or die("MySQL Connection Error\n");
		// TODO create and set default db

		// if(mysql_num_rows(mysql_query("SHOW TABLES LIKE '%" . $table . "%'")==1))
	}

	private function connectToDatabase()
	{
		$this->conn = new mysqli(SERVER_ADDRESS, USER_NAME, PASSWORD, DBNAME);
		if($this->conn->connect_error !== null)
		{
			$this->logConnectFailure($this->conn->connect_error);
			return false;
		}
		$this->conn->set_charset('utf8mb4');
		$this->conn->query('SET @@SESSION.sql_mode = "traditional";');
		return true;
	}

	function __destruct()
	{
		if(!$this->hasLoggedEnd)
		{
			$this->logEnd(false, 0, PHP_INT_MAX);
		}
		$this->conn->close();
		curl_close($this->ch);
		fclose($this->fp);
	}

	function run()
	{
		$this->crawlAllCids();
	}

	private function log($msg, $isWritingFile = true)
	{
		echo $msg;
		if($isWritingFile)
		{
			fwrite($this->fp, $msg);
		}
	}

	private function logWithState($msg, $isSevere = false)
	{
		$msg = " ".$msg;
		if($isSevere)
		{
			$msg = "[SEVERE]".$msg."\n";
		}
		$msg = date("[y/m/d H:i:s]").$msg."\n";
		$this->log($msg);
	}

	private function logStart($start, $end)
	{
		$this->startTime = time();
		$this->logWithState("Start to crawl comments from cid: $start to cid: $end ");
	}

	private function logFailure($action, $isSevere = true)
	{
		$msg = "At cid: $this->cid failed to: $action ";
		if($isSevere)
		{
			if(!$this->hasFailed)
			{
				$this->skippedCids[] = $this->cid;
				$this->hasFailed = true;
			}
		}
		$this->logWithState($msg, $isSevere);
	}

	private function logRequestFailure($error, $url)
	{
		$this->logFailure("request content of url: $url for: $error");
	}

	private function logRetry($reason, $num = -1)
	{
		$msg = "Try to $reason";
		if($num >= 1)
		{
			$msg .= "for a $num time";
		}
		$this->logWithState($msg);
	}

	private function logErrorXml()
	{
		$this->logFailure("request indeed xml file, having got a xml file containing only one 'error' node");
	}

	private function logCrawlHistoryFailure()
	{
		$this->logFailure("crawl comments history");
	}

	private function logBadFormat($badFormat, $url)
	{
		$this->logFailure("get well-formatted file, having got $badFormat instead", false);
	}

	private function logParseFailure($error, $url)
	{
		$this->logFailure("parse xml file: $error->message in line: $error->line from url: $url");
	}

	private function logQueryFailure($error, $queryString)
	{
		$this->logFailure("query database for: $error with: $queryString");
	}

	private function logConnectFailure($error)
	{
		$this->logFailure("connect to database for: $error");
	}

	private function logEnd($isSuccessful, $start, $end)
	{
		$this->hasLoggedEnd = true;
		$msg = '';
		if($isSuccessful)
		{
			$msg .= "Successful termination ";
		}
		else
		{
			$msg .= "Unexpeced termination ";
		}
		if($this->cid < $end)
		{
			$end = $this->cid;
		}
		$total = $end - $start + 1;
		$msg .= "\n                    Cid: from $start to $end, total $total";
		$msg .= "\n                    Duration: ".self::formatDateDiff('@'.$this->startTime);
		// $msg .= "\n              Speed: ";
		$tmp = count($this->skippedCids);
		if($tmp > 0)
		{
			$msg = $msg."\n                    $tmp cids go wrong ";
			$msg = $msg."\n                    ".implode(", ", $this->skippedCids);
		}
		else
		{
			$msg = $msg."\n                    All cids are crawled successfully! ";
		}
		$this->logWithState($msg);
	}

	/**
	 *
	 * Gets contents from a bilibili.com website
	 *
	 * @param string $url the URL to get content
	 * @return string|boolean string for contents got, and FALSE on failure
	 *
	 */
	function getUrlContents($url)
	{
		curl_setopt($this->ch, CURLOPT_URL, $url);
		$numFailure = 0;
		while(true)
		{
			$raw = curl_exec($this->ch);
			if($raw !== false)
			{
				return $raw;
			}

			$numFailure++;
			if($numFailure >= self::MAX_REQUEST_TIMES)
			{
				$this->logRequestFailure(curl_error($this->ch), $url);
				return false;
			}
			sleep(self::RELAX_DURATION);
			$this->logRetry('request ', $numFailure);
		}
	}

	/**
	 *
	 * Gets the xml file of current cid, which could be an up-to-date or historical version, on $this->cid.
	 *
	 * @param int $timestamp the latest timestamp of a date upon which the crawler can crawl. If not specified, the cralwer will crawl the up-to-date xml file. Only timestamps in rolldate can be specified
	 * @return DOMDocument|boolean TRUE on nothing to crawl, FALSE on failure, and DOMDocument for the xml file crawled
	 *
	 */
	function getCommentsXml($timestamp = 0)
	{
		$url = 'http://comment.bilibili.com/';
		if($timestamp <= 0) // get current cid's xml
		{
			$url .= "$this->cid.xml";
		}
		else // get certain segment of current cid's history xml
		{
			$url .= "dmroll,$timestamp,$this->cid";
		}
		$xmlString = $this->getUrlContents($url);
		if(!is_string($xmlString))
		{
			return $xmlString;
		}
		$xmlString = self::stripInvalidXmlChars($xmlString);

		$xml = new DOMDocument('1.0', 'UTF-8');
		if($xml->loadXML($xmlString) === false)
		{
			if($xml->loadHTML($xmlString) === true) // probably a 404 error page
			{
				$this->logBadFormat('html', $url);
				$this->updateTableFromXml('ChatMetadata', 'chat_source', null, 'html', false);
				return true;
			}
			$this->logParseFailure(libxml_get_last_error(), $url);
			return false;
		}
		if($xml->childNodes->item(0)->nodeValue === "error")
		{
			$this->logErrorXml();
			return false;
		}

		return $xml;
	}

	/**
	 *
	 * Gets the roll dates json file of current cid
	 *
	 * $rollDates = [
	 *		{
	 *			"timestamp": "", // the bottom-end of the comments list's timestamp, which is a multiplication of 86400
	 * 			"new": "" // the amount of new comments, the value of which may not imply the exact number of newly added comments of this date
	 * 		},
	 * 		...
	 * ];
	 *
	 * @param boolean $isArray whether the parsed json file should be returned as an associative array or an object
	 * @return array|object|boolean FALSE on failure, array on $isArray being true, and object on $isArray being false
	 *
	 */
	function getRollDatesJson($isArray = false)
	{
		$jsonString = $this->getUrlContents("http://comment.bilibili.tv/rolldate,$this->cid");
		if(!is_string($jsonString))
		{
			return $jsonString;
		}
		$rollDates = json_decode($jsonString, $isArray);

		if(json_last_error())
		{
			$this->logFailure('decode json for: '.json_last_error_msg());
			return false;
		}
		if(count($rollDates) === 0)
		{
			return false;
		}
		foreach($rollDates as $key => $rollDate)
		{
			if(!property_exists($rollDate, 'timestamp') || $rollDate->timestamp <= 0 ||!property_exists($rollDate, 'new') || $rollDate->new <= 0)
			{
				$this->logFailure("get legal json object with: ".print_r($rollDate));
				return false;
			}
		}
		return $rollDates;
	}

	/**
	 *
	 * Crawls history comments on $this->cid within an adjustable range.
	 * Since the 'new' property of $rollDate is unreliable, the crawler approximates to get all history comments in fewest possible times of fetching, which may result in skipping fetching part of history comments when occasionally the 'timestamp' property of $rollDate becomes unreliable as well
	 *
	 * @param int $headCommentTimestamp the latest timestamp upon which the cralwer can crawl
	 * @param int $headCommentId the latest comment ID upon which the crawler can crawl
	 * @param boolean $isCrawlingAll should the crawler crawl all history comments prior to the head comment or crawl only comments following the head comment in one date
	 * @return boolean TRUE on success and FALSE on failure
	 *
	 */
	function crawlHistoryComments($headCommentTimestamp = PHP_INT_MAX, $headCommentId = PHP_INT_MAX, $isCrawlingAll = true)
	{
		$rollDates = $this->getRollDatesJson();
		if($rollDates === false)
		{
			return false;
		}

		// loop through all rollDates to get comments and to insert them into database
		$maxLimit = 0;
		for($numRollDate = count($rollDates) - 1; $numRollDate >= 0; $numRollDate--)
		{
			// look for the desirable the where the breakpoint locates or the first date
			if($numRollDate >= 1)
			{
				$nextRollDateTimestamp = $rollDates[$numRollDate - 1]->timestamp;
				if($nextRollDateTimestamp > $headCommentTimestamp)
				{
					continue;
				}
			}

			// get uniserted comment entries of the date
			$segmentOfHistoryCommentsXml = $this->getCommentsXml($rollDates[$numRollDate]->timestamp);
			if(!($segmentOfHistoryCommentsXml instanceof DOMDocument))
			{
				return $segmentOfHistoryCommentsXml;
			}
			$commentEntries = self::getSortedCommentsArray($segmentOfHistoryCommentsXml);
			for($i = count($commentEntries) - 1; $i >= 0; $i--)
			{
				if($commentEntries[$i][self::CMTENT_ATTRIS][self::CMT_ID] < $headCommentId)
				{
					$commentEntries = array_slice($commentEntries, 0, $i + 1);
					break;
				}
			}

			$this->insertCommentEntries($commentEntries);

			if(!$isCrawlingAll) // should be replaced by end timestamp and id
			{
				break;
			}

			// update the breakpoint
			$headComment = array_shift($commentEntries);
			if(!$headComment)
			{
				continue;
			}
			$headCommentTimestamp = $headComment[self::CMTENT_ATTRIS][self::CMT_DATE];
			$headCommentId = $headComment[self::CMTENT_ATTRIS][self::CMT_ID];
		}

		return true;
	}

	/**
	 *
	 * Crawls on $this->cid by getting the comments and inserting them into database.
	 *
	 * @return boolean TRUE on success and FALSE on failure
	 *
	 */
	function crawlComments()
	{
		// start crawling
		$this->query("INSERT INTO ChatMetadata(chat_id) VALUES($this->cid)");

		// get the xml file of current comments and the info of this chat id
		$commentsXml = $this->getCommentsXml();
		if(!($commentsXml instanceof DOMDocument))
		{
			return $commentsXml;
		}

		// update the ChatMetadata table
		$this->updateTableFromXml('ChatMetadata', 'chat_source', $commentsXml, 'source');
		$this->updateTableFromXml('ChatMetadata', 'chat_max_limit', $commentsXml, 'maxlimit');
		$this->updateTableFromXml('ChatMetadata', 'chat_max_count', $commentsXml, 'max_count');
		$this->updateTableFromXml('ChatMetadata', 'chat_mission', $commentsXml, 'mission');

		// insert comments from the xml file into database
		$commentEntries = self::getSortedCommentsArray($commentsXml);
		if(count($commentEntries) === 0)
		{
			return true;
		}
		if($this->insertCommentEntries($commentEntries) === false)
		{
			return false;
		}

		// crawl the xml files of history comments
		$maxLimitNode = $commentsXml->getElementsByTagName("maxlimit");
		if($maxLimitNode->length === 0)
		{
			return true;
		}
		$maxLimit = $maxLimitNode->item(0)->nodeValue;
		if($maxLimit > 0 && count($commentEntries) >= $maxLimit)
		{
			$this->crawlHistoryComments($commentEntries[0][self::CMTENT_ATTRIS][self::CMT_DATE], $commentEntries[0][self::CMTENT_ATTRIS][self::CMT_ID]);
		}

		return true;
	}

	/**
	 *
	 * Crawls on a range of or [TODO] an array of cids specified by the ctor.
	 *
	 * @return void
	 *
	 */
	function crawlAllCids() // while others crawl comments at a single cid, only this method crawls comments at multiple cids
	{
		$start = $this->start;
		$end = $this->end;

		$this->logStart($start, $end);

		$numFailure = 0;
		$isSuccessful = true;
		$total = $end - $start + 1;
		for($i = $start; $i <= $end; $i++)
		{
			$this->cid = $i;
			$this->hasFailed = false;

			$completed = $i - $start;
			if(rand(0, $total) < 120 || $completed === 0)
			{
				$this->logWithState('progress: '.floor($completed / $total * 100)."%, [$completed/$total]");
			}

			if($this->crawlComments() === false)
			{
				$numFailure++;
				if($numFailure >= self::MAX_CIDS_SKIPPED)
				{
					$isSuccessful = false;
					break;
				}
			}
			else if($numFailure > 0)
			{
				$numFailure--;
			}

			$hour = date('G');
			if($hour > 19 && $hour < 21)
			{
				sleep(RELAX_DURATION);
			}
		}

		$this->logEnd($isSuccessful, $start, $end);
	}

	/**
	 *
	 *  $attriArray => array(
	 *  	[0] => cmt_time
	 *  	[1] => cmt_mode
	 *  	[2] => cmt_size
	 *  	[3] => cmt_color
	 *  	[4] => cmt_date
	 *  	[5] => cmt_pool
	 *  	[6] => cmt_user_id
	 *  	[7] => cmt_id
	 *  )
	 *
	 */
	private static function getSortedCommentsArray($xml)
	{
		$commentsArray = array();
		$commentsArrayKeeper = null;
		$lastCommentId = PHP_INT_MIN;
		$hasFoundGap = false;
		foreach($xml->getElementsByTagName('d') as $key => $DOMNode)
		{
			$commentEntry = array();
			$commentEntry[self::CMTENT_ATTRIS] = explode(',', $DOMNode->attributes->item(0)->value);
			$commentEntry[self::CMTENT_CONTENT] = $DOMNode->nodeValue;
			if(!$hasFoundGap)
			{
				// there may be some protected comments which have way smaller IDs than those of common comments at the end of a comment xml file
				if($commentEntry[self::CMTENT_ATTRIS][self::CMT_ID] < $lastCommentId)
				{
					$hasFoundGap = true;
					$commentsArrayKeeper = $commentsArray;
					$commentsArray = array();
				}
				else
				{
					$lastCommentId = $commentEntry[self::CMTENT_ATTRIS][self::CMT_ID];
				}
			}
			$commentsArray[] = $commentEntry;
		}

		usort($commentsArray, array('Cralwer', 'compareCommentEntries'));
		if($commentsArrayKeeper)
		{
			usort($commentsArrayKeeper, array('Cralwer', 'compareCommentEntries'));
			$commentsArray = array_merge($commentsArrayKeeper, $commentsArray);
		}
		return $commentsArray;
	}

	private static function compareCommentEntries($entry1, $entry2)
	{
		return $entry1[0][self::CMT_ID] - $entry2[0][self::CMT_ID];
	}

	private function insertCommentEntries($commentEntries)
	{
		$commentsQuery = 'INSERT IGNORE INTO Comments(cmt_id,cmt_date,cmt_user_id,CMT_CONTENT,chat_id,cmt_time,cmt_mode,cmt_size,cmt_color,cmt_pool)';
		$specialCommentsQuery = 'INSERT IGNORE INTO SpecialComments(cmt_id,cmt_date,cmt_user_id,CMT_CONTENT,chat_id)';
		$commentsArray = array();
		$specialCommentsArray = array();

		foreach($commentEntries as $key => $commentEntry)
		{
			$attriArray	= $commentEntry[self::CMTENT_ATTRIS];
			$contentText = $this->conn->real_escape_string($commentEntry[self::CMTENT_CONTENT]);
			$queryString = "(".$attriArray[self::CMT_ID].",FROM_UNIXTIME(".$attriArray[self::CMT_DATE]."),'".$attriArray[self::CMT_USER_ID]."','$contentText',$this->cid";
			if($attriArray[self::CMT_POOL] == 2)
			{
				$queryString .= ')';
				$specialCommentsArray[] = $queryString;
			}
			else
			{
				$queryString .= ",".$attriArray[self::CMT_TIME].",".$attriArray[self::CMT_MODE].",".$attriArray[self::CMT_SIZE].",".$attriArray[self::CMT_COLOR].",".$attriArray[self::CMT_POOL].")";
				$commentsArray[] = $queryString;
			}
		}

		$result = true;
		if(count($commentsArray) > 0)
		{
			$commentsQuery .= " VALUES".implode(',', $commentsArray).";";
			$result = $result && $this->query($commentsQuery);
		}
		if(count($specialCommentsArray) > 0)
		{
			$specialCommentsQuery .= " VALUES".implode(',', $specialCommentsArray).";";
			$result = $result && $this->query($specialCommentsQuery);
		}
		return $result;
	}

	private function query($queryString)
	{
		$respond = $this->conn->query($queryString);
		if($respond === false)
		{
			switch ($this->conn->errno)
			{
				case 2006:
				case 2013: // the server has gone
					$this->logFailure('Mysql connection has gone, trying to reconnect');
					$this->conn->close();
					if(!$this->connectToDatabase())
					{
						$this->logEnd(false, 0, PHP_INT_MAX);
						die();
					}
					$this->logWithState('Connection re-established');
					$respond = $this->conn->query($queryString);
					break;
				default:
					$this->logQueryFailure($this->conn->error, $queryString);
					break;
			}
		}
		return $respond;
	}

	private function updateTableFromXml($tableName, $columnName, $xml, $element, $isName = true)
	{
		if($isName === true)
		{
			$xml = $xml->getElementsByTagName($element);
			if($xml->length !== 1)
			{
				return false;
			}
			$element = $this->conn->real_escape_string($xml->item(0)->nodeValue);
		}

		return $this->query("UPDATE $tableName SET $columnName = '$element' WHERE chat_id = $this->cid;");
	}

	private static function stripInvalidXmlChars($value)
	{
	    $ret = "";
	    $current;
	    if (empty($value))
	    {
	        return $ret;
	    }

	    $length = strlen($value);
	    for ($i=0; $i < $length; $i++)
	    {
	        $current = ord($value{$i});
	        if (($current == 0x9) ||
	            ($current == 0xA) ||
	            ($current == 0xD) ||
	            (($current >= 0x20) && ($current <= 0xD7FF)) ||
	            (($current >= 0xE000) && ($current <= 0xFFFD)) ||
	            (($current >= 0x10000) && ($current <= 0x10FFFF)) ||
	            ($current == 0xFFFF) // how come?
	            )
	        {
	            $ret .= chr($current);
	        }
	        else
	        {
	            $ret .= "\x".dechex($current);
	        }
	    }
	    return $ret;
	}

	private static function formatDateDiff($start, $end=null)
	{
	    if(!($start instanceof DateTime)) {
	        $start = new DateTime($start);
	    }

	    if($end === null) {
	        $end = new DateTime();
	    }

	    if(!($end instanceof DateTime)) {
	        $end = new DateTime($end);
	    }

	    $interval = $end->diff($start);
	    $doPlural = function($nb,$str){return $nb>1?$str.'s':$str;}; // adds plurals

	    $format = array();
	    if($interval->y !== 0) {
	        $format[] = "%y ".$doPlural($interval->y, "year");
	    }
	    if($interval->m !== 0) {
	        $format[] = "%m ".$doPlural($interval->m, "month");
	    }
	    if($interval->d !== 0) {
	        $format[] = "%d ".$doPlural($interval->d, "day");
	    }
	    if($interval->h !== 0) {
	        $format[] = "%h ".$doPlural($interval->h, "hour");
	    }
	    if($interval->i !== 0) {
	        $format[] = "%i ".$doPlural($interval->i, "minute");
	    }
	    if($interval->s !== 0) {
	        if(!count($format)) {
	            return "less than a minute ago";
	        } else {
	            $format[] = "%s ".$doPlural($interval->s, "second");
	        }
	    }

	    // We use the two biggest parts
	    if(count($format) > 1) {
	        $format = array_shift($format)." and ".array_shift($format);
	    } else {
	        $format = array_pop($format);
	    }

	    // Prepend 'since ' or whatever you like
	    return $interval->format($format);
	}
}


$crawler = new Cralwer($argc, $argv);

$crawler->crawlAllCids();


?>