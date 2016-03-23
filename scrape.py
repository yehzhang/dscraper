from dscraper.utils import CommentFlow
from dscraper.company import CommentWorker


import sys
import dscraper
import asyncio
import logging

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# hard-coded default config
DEFAULT_LOGGING_CONFIG = {}
DEFAULT_SCRAPER_CONFIG = {}
# TODO
# config from config file
# config from command-line

def main():
	# TODO
	# get command line arguments
	# configure logging, update default with file
	# configure scraper, update default with file
	# scrape
	loop = asyncio.get_event_loop()
	exporter = dscraper.FileExporter(merge=True, loop=loop)
	scraper = dscraper.Scraper(exporter, max_workers=6, loop=loop)
	# scraper.add_range(1, 50)
	# scraper.add_list([128,132,183,430,440,590])
	scraper.add(6150685)
	scraper.run()

if __name__ == '__main__':
	main()
