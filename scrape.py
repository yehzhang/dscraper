#! /usr/bin/env python3.5
import os
import dscraper
import asyncio
import logging
import logging.handlers
import argparse
import datetime

# TODO hard-coded default config
DEFAULT_SCRAPER_CONFIG = {}


FILE = 'file'
STDOUT = 'stdout'
MYSQL = 'mysql'

LOGGING_DIR = './log'

logger = None


def get_args():
    # TODO config from config file and from command-line
    raise NotImplementedError


def parse_args():
    # TODO mode: add AID; export: add mysql
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--export', metavar='method', default='file',
                        choices=[FILE, STDOUT], help='how is data exported')
    parser.add_argument('-p', '--path', metavar='path', default='./comments',
                        help='where should files go if -e "file" was specified')
    parser.add_argument('-m', '--merge', action='store_true', default=False,
                        help='whether merging comments on different dates as one file if -e "file" was specified')

    parser.add_argument('--no-history', dest='history', action='store_false', default=True,
                        help='do not scrape history comments. Scrape latest comments only')
    parser.add_argument('-t', '--type', metavar='type', default='cid',
                        choices=['cid'], help='what kind of ID numbers are specified in -r and targets')
    parser.add_argument('-s', '--start-time', metavar='start', dest='start', type=int, default=None,
                        help='unix timestamp specifying the starting time of comments to scrape (inclusive)')
    parser.add_argument('-n', '--end-time', metavar='end', dest='end', type=int, default=None,
                        help='unix timestamp specifying the ending time of comments to scrape (inclusive)')

    parser.add_argument('-r', '--range', metavar=('first', 'last'), nargs=2, type=int, action='append', default=[],
                        help='the first and last ID numbers of consecutive targets to scrape. Can be specified multiple times')
    parser.add_argument('targets', nargs='*', type=int,
                        help='ID numbers of individual targets to scrape')

    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='logging in a really verbose way')

    args = parser.parse_args()
    if not (args.range or args.targets):
        parser.error('no targets specified: expected --range and/or targets')
    return args


def config_logging(verbose):
    def set_handler(hdlr):
        hdlr.setLevel(lvl)
        hdlr.setFormatter(fmt)
        logger.addHandler(hdlr)
    name = dscraper.__name__
    lvl = logging.INFO if not verbose else logging.DEBUG
    fmt = logging.Formatter('%(asctime)s %(levelname)s - %(message)s', '%m-%d %H:%M')
    global logger
    logger = logging.getLogger(name)
    logger.setLevel(lvl)

    now = datetime.datetime.now()
    log_path = os.path.join(os.path.abspath(LOGGING_DIR), now.strftime('%b_%d'))
    os.makedirs(log_path, exist_ok=True)
    filename = os.path.join(log_path, '{}_{}.log'.format(now.strftime('%d-%m-%y-%H00'), name))
    set_handler(logging.handlers.RotatingFileHandler(filename, maxBytes=10 * 2**20, backupCount=5))

    set_handler(logging.StreamHandler())


def main():
    args = parse_args()
    export, path, start, end, mode, range_targets, targets, merge, history, verbose = \
        args.export, args.path, args.start, args.end, args.type, args.range, args.targets, \
        args.merge, args.history, args.verbose
    time_range = None if start is None and end is None else (start, end)

    config_logging(verbose)

    loop = asyncio.get_event_loop()

    if export == FILE:
        exporter = dscraper.FileExporter(path, merge, loop=loop)
    elif export == STDOUT:
        exporter = dscraper.StreamExporter(loop=loop)
    elif export == MYSQL:
        # TODO generate config file and notice user to set username and pw in config file
        pass

    scraper = dscraper.Scraper(exporter, history, time_range, loop=loop)
    mode = mode.upper()
    for target in targets:
        scraper.add(target, mode)
    for start, end in range_targets:
        scraper.add_range(start, end, mode)

    logger.info('Start scraping with the configuration: export: %s, path: %s, time_range: %s, mode: %s, range_targets: %s, targets: %s, merge: %s, history: %s',
                export, path, time_range, mode, range_targets, targets, merge, history)
    scraper.run()

    loop.close()

if __name__ == '__main__':
    main()
