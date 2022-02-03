#!/usr/bin/env python2
"""
Two possibilities to get loglines:
    * Receive loglines from printf-uart-nanomsg.py by serving a nanomsg REP socket.
    * Subscribe a nanomsg SUB socket to a nanoprintf-server.py PUB socket.

Forwards all loglines to a nanomsg PUB socket.

Logs all loglines to local logfiles. Separate file for every hostname/port.
"""

import sys
import time
import errno
import datetime

from nanomsg import Socket, PUB, SUB, REP, SUB_SUBSCRIBE
from nanomsg import SOL_SOCKET, RECONNECT_IVL, RECONNECT_IVL_MAX, DONTWAIT, NanoMsgAPIError

import logging
import logging.handlers
from .watchedlogger import WatchedTimedRotatingFileHandler

__author__ = "Elmo Trolla, Mattis Marjak, Andres Vahter, Raido Pahtma"
__license__ = "MIT"

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)-5s: %(message)s"
)

# dict of opened loggers. one for every device/logfile
g_loggers = {}


def create_logger(filename):
    """ create/return a logger that logs to a file with the given name. rotate the file every midnight, keep 14 days
    l = create_logger("hello.log"); l.info("line1"); # creates a file "hello.log" with only "line1\n" for content. """
    logformat = logging.Formatter("%(message)s")
    logfile = WatchedTimedRotatingFileHandler(
        filename,
        when="midnight",
        utc=True,
        backupCount=14
    )
    logfile.setFormatter(logformat)
    newlogger = logging.getLogger(filename)
    newlogger.addHandler(logfile)
    newlogger.setLevel(logging.DEBUG)
    return newlogger


def write_to_log(msg):
    """
    "hostname_portname 123ABC x2014-01-14T14:43:21.23Z this is the original line"

    123ABC - seqno
    the x means the line is corrupt (no newline from uart for too long)
    """
    hostname_n, seqno, rest = msg.split(None, 2)

    # create one logger for every hostname_n (unique for each host and serial port)
    # the logger will create one rotated log file for itself.
    if hostname_n not in g_loggers:
        logfilename = "log_%s.log" % hostname_n
        g_loggers[hostname_n] = create_logger(logfilename)
        sys.stdout.write("\n")
        log.info("opening log file %s", logfilename)

    g_loggers[hostname_n].debug(rest)


def run(addr_listenprintf, addr_forward, addr_subscribe, uselog, debug, binary):

    log.info("listening for printf-uart-nanomsg: %s", addr_listenprintf)
    log.info("subscribing for messages to      : %s", addr_subscribe)
    log.info("forwarding messages to           : %s", addr_forward)
    log.info("logging messages to files        : %s", uselog)
    log.info("logging messages to stdout       : %s", debug)
    if binary:
        log.info("Using binary mode")
    

    if not debug:
        logging.getLogger().handlers[0].setLevel(logging.INFO)

    soc_rep = None
    soc_pub = None
    soc_sub = None

    if addr_listenprintf and addr_listenprintf.lower() != "none":
        soc_rep = Socket(REP)
        soc_rep.bind(addr_listenprintf)

    if addr_forward and addr_forward.lower() != "none":
        soc_pub = Socket(PUB)
        soc_pub.bind(addr_forward)

    if addr_subscribe and addr_subscribe.lower() != "none":
        soc_sub = Socket(SUB)
        soc_sub.set_string_option(SUB, SUB_SUBSCRIBE, "")
        # start reconnecting after one second pause
        # max reconnect timer to 30 seconds
        soc_sub.set_int_option(SOL_SOCKET, RECONNECT_IVL, 1000)
        soc_sub.set_int_option(SOL_SOCKET, RECONNECT_IVL_MAX, 1000 * 30)
        soc_sub.connect(addr_subscribe)

    while 1:
        # read from addr_listenprintf and forward to addr_forward

        if soc_rep:

            jumbomsg = None
            try:
                jumbomsg = soc_rep.recv(flags=DONTWAIT)
            except NanoMsgAPIError as e:
                if e.errno != errno.EAGAIN:
                    raise

            if jumbomsg:
                # reply anything to the REQ socket or no more messages will arrive
                soc_rep.send("got it")
                # ONLY messages from the REQ socket can be jumbomessages (simple lines joined by newlines)
                msgs = None
                if binary:
                    msgs = jumbomsg.decode('ascii').split("\n")
                else:
                    msgs = jumbomsg.split("\n")
                
                hostname_n = "?"
                for msg in msgs:
                    hostname_n, rest = msg.split(None, 1)
                    if soc_pub:
                        soc_pub.send(msg)
                    if uselog:
                        write_to_log(msg)

                t = datetime.datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%dT%H:%M:%S.%f")[:22] + "Z"
                sys.stdout.write("{} {}: {}\n".format(t, hostname_n, len(msgs)))
                sys.stdout.flush()

        # read from addr_subscribe and forward to addr_forward

        if soc_sub:
            while 1:
                try:
                    msg = soc_sub.recv(flags=DONTWAIT)
                except NanoMsgAPIError as e:
                    if e.errno == errno.EAGAIN:
                        break
                    else:
                        raise

                if msg:
                    hostname_n, rest = msg.split(None, 1)
                    sys.stdout.write(hostname_n[-1])
                    sys.stdout.flush()
                    if soc_pub:
                        soc_pub.send(msg)
                    if uselog:
                        write_to_log(msg)

        time.sleep(0.01)


def main():
    from argparse import ArgumentParser
    ap = ArgumentParser(
        description="printf-uart-nanomsg receiver.\n ...."
    )
    ap.add_argument(
        "--listenprintf",
        dest="addr_listenprintf",
        default="tcp://*:14999",
        help=(
            "printf-uart-nanomsg connects here. using 'None' "
            "turns the listener off. default: tcp://*:14999"
        )
    )
    ap.add_argument(
        "--forward",
        dest="addr_forward",
        default="tcp://*:14998",
        help=(
            "log subscribers connect here. using 'None' "
            "turns off. default: tcp://*:14998"
        )
    )
    ap.add_argument(
        "--subscribe",
        dest="addr_subscribe",
        default=None,
        help=(
            "pull messages from another nanoprintf-server. "
            "disabled by default, enable with: tcp://host:14998"
        )
    )
    ap.add_argument(
        "--log",
        dest="uselog",
        action="store_true",
        help="write incoming messages to log files"
    )
    ap.add_argument(
        "--rotate",
        dest="uselog",
        action="store_true",
        help="write incoming messages to log files"
    )
    ap.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="write incoming messages to stdout"
    )
    ap.add_argument(
        "--binary",
        dest="binary",
        default=False,
        help="Binary mode toggle",
    )
    args = ap.parse_args()
    run(**args.__dict__)


if __name__ == "__main__":
    main()
