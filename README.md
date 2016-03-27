# dscraper
A scraper that helps you get both latest and history comments on Bilibili. The purpose of this project is to get data for research and to download full comments from different videos for migration. 

## Features
+ Get history comments with minimum requests, skipping, or completing when exporting as files, repeated occurrences of the same comment
+ Basic manipulations on data such as filtering and joining
+ Built upon asyncio for efficiency
+ Logging, exception handling, and automatic retrying
+ Slow down at rush hour (before getting blocked)

### TODO list
+ Export data to MySQL and other databases
+ Interface for adding / changing / pausing targets when running
+ Get comments by AID (now support only for specifying CID)

## Installation
```
$ git clone https://github.com/yehzhang/dscraper.git
$ cd dscraper
$ pip3 install -r requirements.txt
```

### Dependency
+ pytz

## Usage
To run this script, make sure you have Python 3.5 installed.

Save full comments from CID 6319180, 6437766, and 6536047 to ./comments:
```
$ ./scrape.py 6319180 6437766 6536047
```

Save comments created between Feb 27 and Mar 27 (GMT+8) from CID 1000 to 2000, and keep one file for each CID:
```
$ ./scrape.py -s 1456560000 -n 1459065600 -r 1000 2000 -m
```

See also ./scrape.py -h
