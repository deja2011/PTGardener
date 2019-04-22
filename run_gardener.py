#!/usr/bin/env python3

import argparse
import logging
import sys

from Gardener import Gardener


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="Path to config file")
    parser.add_argument("-i", "--interval", type=int, default=0,
                        help="Time interval (in seconds) to refresh torrents and patterns, "
                        "if not positive will only refresh once, defaults to 0 i.e. only refresh once")
    parser.add_argument("-l", "--log", default="run_gardener.log",
                        help="Path to log file, defaults to ./run_gardener.log")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(filename=args.log,
                        level=(logging.DEBUG if args.debug else logging.INFO),
                        format="%(asctime)s %(levelname)s %(module)s %(message)s")
    logging.info("Launched {}")
    logging.info("Command line arguments: {}".format(" ".join(sys.argv)))
    gardener = Gardener(config_file=args.config)
    gardener.run(interval=args.interval)


if __name__ == "__main__":
    main()
