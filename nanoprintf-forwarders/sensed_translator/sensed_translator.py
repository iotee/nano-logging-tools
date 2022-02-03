#!/usr/bin/env python2

"""
Subscribe to nanoprintf-server and listen for connections from sensed on a PUB socket.
"""

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.NOTSET, format="%(asctime)s %(name)s %(levelname)-5s: %(message)s")

import sys
import time
import errno
import calendar

from nanomsg import Socket, PUB, SUB, REP, SUB_SUBSCRIBE
from nanomsg import SOL_SOCKET, RECONNECT_IVL, RECONNECT_IVL_MAX, DONTWAIT, NanoMsgAPIError
from nuggets import nuggets

def timestr_to_timestamp(timestr):
    """timestr format: '2014-02-11T18:46:22.13Z'"""
    frac = float(timestr[20:22]) / 100.
    return calendar.timegm(time.strptime(timestr[:19], "%Y-%m-%dT%H:%M:%S")) + frac


def transform_for_sensed(msg):

    # take apart lines like this
    # koerkana1_4 123ABC 2015-01-16 13:25:05.25Z 'N-cbuf 2B45 01 0C'

    try:
        hostname_n, seqno, timestr, rest = msg.split(None, 3)

        if timestr.startswith("x"):
            return

        timestamp = timestr_to_timestamp(timestr)

        # remove quotes
        rest = rest[1:-1]

        # use the koerkana index as node name. take the num 1 from koerkana1_4.
        name = hostname_n[8] if hostname_n.startswith("koerkana") else "-"

        nugget = rest
        if nugget.startswith("N-"):
            # yay. we have a sensed line!
            p = nugget.split()
            for nugget in nuggets:
                if p[0] == nugget['prefix']:
                    fields = dict(zip(nugget['fields'], p))
                    params = [timestamp, int(fields['node'],16), fields['name']]+nugget['prep'](fields)
                    return nugget['frmt'] % params
            log.error("unknown sensed packet: %s", msg)
    except:
        log.exception("error parsing msg: %s", msg)


def run(addr_forward, addr_subscribe):

    log.info("subscribing for messages to      : %s", addr_subscribe)
    log.info("forwarding messages to PUB       : %s", addr_forward)

    soc_pub = None
    soc_sub = None

    soc_pub = Socket(PUB)
    soc_pub.bind(addr_forward)

    soc_sub = Socket(SUB)
    soc_sub.set_string_option(SUB, SUB_SUBSCRIBE, "")
    # start reconnecting after one second pause
    # max reconnect timer to 30 seconds
    soc_sub.set_int_option(SOL_SOCKET, RECONNECT_IVL, 1000)
    soc_sub.set_int_option(SOL_SOCKET, RECONNECT_IVL_MAX, 1000 * 30)
    soc_sub.connect(addr_subscribe)

    while 1:
        # read from addr_subscribe and forward to addr_forward

        if soc_sub:

            msg = None
            try:
                msg = soc_sub.recv(flags=DONTWAIT)
            except NanoMsgAPIError as e:
                if e.errno != errno.EAGAIN:
                    raise
                else:
                    time.sleep(0.01)

            if msg:
                #hostname_n, rest = msg.split(None, 1)
                print(msg)

                msg2 = transform_for_sensed(msg)
                if msg2:
                    soc_pub.send(msg2)


if __name__ == "__main__":
    from argparse import ArgumentParser
    ap = ArgumentParser(description="translates messages from nanoprintf-server to sensed.")
    ap.add_argument("--forward", dest="addr_forward", default="tcp://*:55555", help="sensed connects here. default: tcp://*:55555")
    ap.add_argument("--subscribe", dest="addr_subscribe", default="tcp://localhost:14998",
                    help="pull messages from this nanoprintf-server. default: tcp://localhost:14998")
    args = ap.parse_args()
    run(**args.__dict__)
