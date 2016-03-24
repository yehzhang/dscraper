#! /usr/bin/env python3.5
import os
import dscraper
import asyncio
import logging, logging.handlers
import argparse
import datetime

# TODO hard-coded default config
DEFAULT_SCRAPER_CONFIG = {}


FILE = 'file'
STDOUT = 'stdout'
MYSQL = 'mysql'

LOGGING_DIR = './log'


def get_args():
    # TODO config from config file and from command-line
    raise NotImplementedError


def parse_args():
    # TODO mode: add AID; export: add mysql, can be specified multiple times
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--export', metavar='method', required=False,
                        default='file', choices=[FILE, STDOUT], help='how is data exported')
    parser.add_argument('-p', '--path', metavar='path', required=False,
                        default='./comments', help='where should files go if -e "file" was specified')
    parser.add_argument('-m', '--merge', metavar='merge', required=False, type=bool, action='store_true'
                        default=False, help='''whether merging comments of different dates as one
                            file if -e "file" was specified''')

    parser.add_argument('--no-history', dest='history', action='store_false', default=True,
                        help='do not scrape history comments. Scrape latest comments only')
    parser.add_argument('-t', '--type', metavar='type', required=False, default='cid',
                        choices=['cid'], help='what kind of ID numbers are specified in -r and targets')
    parser.add_argument('-d', '--dates', metavar=('begin', 'end'), nargs=2, required=False,
                        type=int, default=(None, None), help='''two unix timestamps specifying
                            the beginning and ending dates between which comments should be
                            scraped (inclusive)''')
    parser.add_argument('-r', '--range', metavar=('first', 'last'), required=False, nargs=2,
                        type=int, action='append', default=[], help='''the first and last ID
                            numbers of consecutive targets to scrape. Can be specified multiple
                            times''')
    parser.add_argument('targets', nargs='*', type=int,
                        help='ID numbers of individual targets to scrape')

    args = parser.parse_args()
    if not (args.range or args.targets):
        parser.error('no targets specified: expected --range and/or targets')
    return args


def config_logging():
    def set_handler(hdlr):
        hdlr.setLevel(logging.INFO)
        hdlr.setFormatter(fmt)
        logger.addHandler(hdlr)
    name = dscraper.__name__
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s %(levelname)-7s - %(message)s', '%m-%d %H:%M')

    now = datetime.datetime.now()
    log_path = os.path.join(os.path.abspath(LOGGING_DIR), now.strftime('%b_%d'))
    os.makedirs(log_path, exist_ok=True)
    filename = os.path.join(log_path, '{}_{}.log'.format(now.strftime('%d-%m-%y-%H00'), name))
    set_handler(logging.handlers.RotatingFileHandler(filename, maxBytes=10*2**20, backupCount=5))

    set_handler(logging.StreamHandler())


def main():
    args = parse_args()
    export, path, (start, end), mode, range_targets, targets, merge, history = args.export, \
        args.path, args.dates, args.type, args.range, args.targets, args.merge, args.history

    loop = asyncio.get_event_loop()

    if export == FILE:
        exporter = dscraper.FileExporter(path, merge, loop=loop)
    elif export == STDOUT:
        exporter = dscraper.StdoutExporter(loop=loop)
    elif export == MYSQL:
        # TODO generate config file and notice user to set username and pw in config file
        pass

    scraper = dscraper.Scraper(exporter, history, start, end, loop=loop)
    mode = mode.upper()
    for target in targets:
        scraper.add(target, mode)
    for start, end in range_targets:
        scraper.add_range(start, end, mode)

    scraper.run()

    loop.close()

if __name__ == '__main__':
    config_logging()
    main()
