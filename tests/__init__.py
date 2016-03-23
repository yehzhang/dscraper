import logging
import sys

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
