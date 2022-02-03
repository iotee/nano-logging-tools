#!/usr/bin/env python2
"""
Read data from serial port, split by newlines, prepend hostname and timestr
to every line and send to a nanomsg REP socket.
line format:
    "host1_1  2015-01-14T13:41:37.90 Hello world"
    "host1_4 x2015-01-14T13:41:37.90 Hello world"
The 'x' denotes a broken line (newline not received in time).

If lines correspond to the format "([0-9a-f]*) [DIWE]\|.*", the process assumes that
the first element is a millisecond timestamp and looks for a BOOT marker in the format
"([0-9a-f]*) B\|BOOT". It then corrects timestamps using the boot time and millisecond
timestamp. Lines are marked broken when they correspond to the timestamp format and
boot time is not known. This behaviour can be disabled with the no-mts option.
"""

import os
import re
import sys
import time
import datetime
import errno
import binascii

import serial

from nanomsg import REQ, REQ_RESEND_IVL
from nanomsg import Socket, SOL_SOCKET, RECONNECT_IVL, RECONNECT_IVL_MAX, DONTWAIT, NanoMsgAPIError

import logging

__author__ = "Elmo Trolla, Mattis Marjak, Andres Vahter, Raido Pahtma, Enari Saar"
__license__ = "MIT"

global log

log = logging.getLogger(__name__)

# binary logging handler
import json
from enum import Enum

class LogMessage:
    def __init__(self, linenr, fmt, severity):
        self.m_line_nr = linenr
        self.m_format = fmt
        self.m_severity = severity
    
    def GetFormat(self):
        return self.m_format
    
    def GetLinenr(self):
        return self.m_line_nr

    def GetSeverity(self):
        return self.m_severity


class Module:
    def __init__(self, name, id, logs):
        self.m_logs = logs
        self.m_name = name
        self.m_id = id
    
    def GetName(self):
        return self.m_name
    
    def GetId(self):
        return self.m_id
    
    def GetLogs(self):
        return self.m_logs

def deserilize_logs(logs):
    lgs = []
    for a in logs:
        lgs.append(LogMessage(a['linenr'],a['format'],a['severity']))
    return lgs

def deserilize_scho(file):
    modules = []
    data = open(file)
    js = json.load(data)
    for a in js:
        modules.append(Module(a['name'],a['id'], deserilize_logs(a['logs'])))

    return modules

class DataTypes(Enum):
    INT16 = 0
    INT32 = 1
    INT64 = 2
    UINT16 = 3
    UINT32 = 4
    UINT64 = 5
    POINTER = 6
    DOUBLE = 7
    STRING = 8

class BinaryParser(object):
    def __init__(self, file, delimiter=b'\x7e', include_delimiter=False, timeout=0.2):
        self.timestamp = time.time()
        self.delimiter = delimiter
        self.include_delimiter = include_delimiter
        self.timeout = timeout
        self.buf = bytearray()
        self.lines = []
        self.modules = deserilize_scho(file)
        self.log = log

    def clear(self):
        self.buf =bytearray()
        self.lines = []

    def isFormatElement(self, element):
        predef = ['d','i','u','x','f','s','p']
        for e in predef:
            if e == element.lower():
                return True
    
        return False

    def SplitFormatters(self, string):
        formatters = []
        found_percentage = False
        start = 0
        for element in range(0, len(string)):
            if found_percentage is not True and string[element] == '%':
                start = element
                found_percentage = True
            if found_percentage is True and self.isFormatElement(string[element]) is True:
                formatters.append(string[start+1:element+1])
                start = 0
                found_percentage = False

        return formatters

    def replace_str_index(self, text,start=0, end=0,replacement=''):
        return '%s%s%s'%(text[:start],replacement,text[end+1:])

    def ReplaceFormatters(self, fmt, data):
        found_percentage = False
        res = fmt
        start = 0
        i = 0
        Done = False
        while Done is not True:
            start = 0
            found_percentage = False
            if res.find('%') == -1:
                Done = True
                break
            for element in range(0, len(res)):
                if found_percentage is not True and res[element] == '%':
                    start = element
                    found_percentage = True
                if found_percentage is True and self.isFormatElement(res[element]) is True:
                    res = self.replace_str_index(res, start, element, str(data[i]))
                    i += 1
                    break
        
        # All data is not formated which means there is a buffer included
        if i != len(data):
            print(data)
            string = res +  " 0x" + data[len(data) - 1].hex()
            res = string
        
        return res


    def GetArguments(self, formats, data):
        #Convert formats to datatypes
        args = []
        types = []
        pos = 0
        for d in formats:
            d = d.lower()
            if d.find('ll') != -1:
                if d[-1] == 'i' or d[-1] == 'd':
                    types.append(DataTypes.INT64)
                elif d[-1] == 'u' or d[-1] == 'x':
                    types.append(DataTypes.UINT64)
            elif d.find('l') != -1:
                if d[-1] == 'i' or d[-1] == 'd':
                    types.append(DataTypes.INT32)
                elif d[-1] == 'u' or d[-1] == 'x':
                    types.append(DataTypes.UINT32)
            else:
                if d[-1] == 'i' or d[-1] == 'd':
                    types.append(DataTypes.INT16)
                elif d[-1] == 'u' or d[-1] == 'x':
                    types.append(DataTypes.UINT16)
                elif d[-1] == 'p':
                    types.append(DataTypes.POINTER)
                elif d[-1] == 'f':
                    types.append(DataTypes.DOUBLE)
                elif d[-1] == 's':
                    types.append(DataTypes.STRING)
    
        for type in types:
            if type == DataTypes.INT64:
                value = int.from_bytes(data[pos:pos+8], byteorder='little',signed=True)
                args.append(value)
                pos += 8
            elif type == DataTypes.UINT64:
                value = int.from_bytes(data[pos:pos+8], byteorder='little',signed=False)
                args.append(value)
                pos += 8
            elif type == DataTypes.INT32:
                value = int.from_bytes(data[pos:pos+4], byteorder='little',signed=True)
                args.append(value)
                pos += 4
            elif type == DataTypes.UINT32:
                value = int.from_bytes(data[pos:pos+4], byteorder='little',signed=False)
                args.append(value)
                pos += 4
            elif type == DataTypes.INT16:
                value = int.from_bytes(data[pos:pos+4], byteorder='little',signed=True)
                args.append(value)
                pos += 4
            elif type == DataTypes.UINT16:
                value = int.from_bytes(data[pos:pos+4], byteorder='little',signed=False)
                args.append(value)
                pos += 4
            elif type == DataTypes.POINTER:
                args.append(str(data[pos:pos+4]))
                pos += 4
            elif type == DataTypes.DOUBLE:
                args.append(data[pos:pos+4])
                pos += 4
            elif type == DataTypes.STRING:
                end_pos = pos
                while True:
                    if data[end_pos] == 0:
                        break
                    end_pos +=1
                args.append(data[pos:end_pos])
                pos += (end_pos - pos)


        # There is data left probably buffer
        if len(data) != pos:
            args.append(data[pos:len(data)])

        return args
    
    def get_message(self, line):
        packet = bytearray(line)
        mod_id = int.from_bytes(packet[0:2], byteorder='big', signed=False) #
        line_nr = int.from_bytes(packet[2:4], byteorder='big', signed=False) #
        data = packet[4:len(packet)]
        fmtstring = ""
        formatters = []
        args = []
        res = ""


        for mod in self.modules:
            if mod.GetId() == mod_id:
                for log in mod.GetLogs():
                    if log.GetLinenr() == line_nr:
                        fmtstring = log.GetFormat()
                        formatters = self.SplitFormatters(fmtstring)
                        args = self.GetArguments(formatters,data)
                        res = self.ReplaceFormatters(fmtstring,args)
                        severity_ = ["I","W","E","D"]
                        severity_str = severity_[log.GetSeverity()] + "|" + mod.GetName() + ":" + str(log.GetLinenr()) + "|"
                        res = severity_str + res

        if res == "":
            res = "Undefined log ModID: " + str(mod_id) + " Line nr: " + str(line_nr)
            
        self.log.warn(res)
        return res
    
    def put(self, data):
        if data:
            timestamp = time.time()

            if len(self.buf) == 0:
                self.timestamp = timestamp

            self.buf += data
            while len(self.buf) > 1:
                delim = self.buf[1:].find(self.delimiter)
                if delim == -1:
                    break
                else:
                    t = self.buf[:delim+2]
                    self.buf = self.buf[delim+2:]
                    if not self.include_delimiter:
                        t = t.rstrip(self.delimiter).lstrip(self.delimiter)
                    
                    self.lines.append((self.timestamp, self.get_message(t)))
                    self.timestamp = timestamp

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.lines) > 0:
            timestamp, line = self.lines.pop(0)
            return line
        else:
            if self.buf and time.time() - self.timestamp > self.timeout:
                t = self.buf
                if not self.include_delimiter:
                    t = t.rstrip(self.delimiter).lstrip(self.delimiter)
                self.buf = bytearray()
                return t

        raise StopIteration
    
    def next(self):
        t = self.__next__
        return t


# drop messages that are in buf but older than this.
MAX_MSG_AGE = 30 * 60.0
MAX_ACK_TIMEOUT = 60.0


def log_timestr(t=None):
    """ '2010-01-18T18:40:42.232Z' utc time """
    if t is None:
        t = time.time()
    # return datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S.%f")[:22]
    return datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def prepare_tx_line(portname, seqno, line, timestamp, broken=False):
    """
    output:
        boken = False: "hostname_portname 123ABC 2014-01-14T14:43:21.23Z this is the original line"
        boken = True : "hostname_portname 123ABC x2014-01-14T14:43:21.23Z this is the original line"
    """
    return "%s_%s %06X %s%s %s" % (os.uname()[1], portname, seqno, "x" if broken else "", log_timestr(timestamp), repr(line))


class NewlineParser:
    def __init__(self):
        self.buf = ""
        self.delimiter = "\n"

    def put(self, data):
        self.buf += data

    def __iter__(self):
        return self

    def next(self):
        t = self._get_next_line()
        return t

    def _get_next_line(self):
        while True:
            self.delete = self.buf.find(self.delimiter)
            if self.delete == -1:
                raise StopIteration
            t = self.buf[:self.delete]
            self.buf = self.buf[self.delete+1:]
            return t


def connect(server):
    soc = Socket(REQ)
    # start reconnecting after one second pause
    # max reconnect timer to one minute
    # request resend wait to shorter than the default 60 seconds
    #   (because nanomsg won't retry the send right at the moment of reconnection)
    soc.set_int_option(SOL_SOCKET, RECONNECT_IVL, 1000)
    soc.set_int_option(SOL_SOCKET, RECONNECT_IVL_MAX, 1000 * 60)
    soc.set_int_option(SOL_SOCKET, REQ_RESEND_IVL, 1000 * 10)
    soc.connect(server)  # "tcp://localhost:14999"
    return soc

def encode_hex_line(line):
    return binascii.hexlify(bytearray(line)).decode("ascii", "replace").upper()

def run(server, port="/dev/ttyUSB0", baud=115200, portname=None, mts=True, debug=False, binmode=""):
    """
    Read data from serial port, split by newlines,
    prepend hostname and timestr to every line and
    send to a nanomsg REP socket.
    """
    log.info("using port %s @ %s, sending to server %s", port, baud, server)

    # setup nanomsg
    soc = connect(server)
    soc_waiting_for_ack = None

    if portname is None:
        portname = port[-1]

    while True:
        try:
            # setup serial port and other variables
            serialport = None
            binaryusage = False
            serial_timeout = 0.01 if sys.platform == "win32" else 0

            while serialport is None:
                try:
                    serialport = serial.serial_for_url(port,
                                               baudrate=baud,
                                               bytesize=serial.EIGHTBITS,
                                               parity=serial.PARITY_NONE,
                                               stopbits=serial.STOPBITS_ONE,
                                               timeout=serial_timeout,
                                               xonxoff=False,
                                               rtscts=False, dsrdtr=False,
                                               exclusive=True,
                                               do_not_open=True)
                    serialport.dtr = 0  # Set initial state to 0
                    serialport.open()
                    serialport.flushInput()
                except (serial.SerialException, OSError):
                    serialport = None
                    time.sleep(0.1)

            log.info("Opened %s." % (port))

            if binmode != "":
                log.info("Using binary mode with file: %s" % (binmode))
                parser = BinaryParser(binmode)
                mts = False
                binaryusage = True
            else:
                parser = NewlineParser()
            
            t_last_recv = time.time()

            outbuf_tx_index = 0
            outbuf = []  # (timestamp, "processed line")

            seqno = 0
            bts = None  # BootTimeStamp

            while True:
                if binaryusage: #needs better implementation
                    s = serialport.read_until(parser.delimiter)
                else:
                    s = serialport.read(1000)
                t = time.time()
                if s:
                    t_last_recv = t
                    if binaryusage == True and "BOOT" not in str(s): #todo fix this this is due to dbprintf_ in h1000 network test
                        parser.put(s)
                    else:
                        parser.put(s)

                    for l in parser:
                        ts = t
                        broken = False

                        if mts:
                            try:
                                m = re.search("([0-9a-f]*) B\|BOOT", l)
                                if m is not None:
                                    offs = int(m.group(1), 16) / 1000.0
                                    bts = t - offs
                                    log.info("BOOT %s %s %s", log_timestr(t), offs, log_timestr(bts))
                                else:
                                    m = re.search("([0-9a-f]*) [DIWE]\|.*", l)
                                    if m is not None:
                                        if bts is not None:
                                            ts = bts + int(m.group(1), 16)/1000.0
                                        else:
                                            broken = True
                            except (ValueError, TypeError):
                                log.exception("ts parse")

                            offs = t - ts
                            if abs(offs) > 1:  # 1 second is a lot, must have missed BOOT message
                                broken = True

                            if debug:  # Don't want unnecessary timestamp formatting to take place
                                if bts is not None:
                                    log.debug("%s/%s (%s, %.3f): %s", log_timestr(t), log_timestr(ts),
                                              log_timestr(bts), offs, l)

                        outbuf.append((ts, prepare_tx_line(portname, seqno, l, ts, broken=broken)))
                        seqno += 1

                # # this here is for testing the system if there's no serial port traffic
                # if t - t_last_recv > 0.5:
                #   t_last_recv = t
                #   outbuf.append( (t, prepare_tx_line(portname, seqno, "Hello world %s" % seqno, t)) )
                #   seqno += 1

                # if no newline character arrives after 0.2s of last recv and parser.buf
                # contains data, send out the partial line.
                if t - t_last_recv > 0.2 and parser.buf:
                    outbuf.append((t, prepare_tx_line(portname, seqno, parser.buf, t, broken=True)))
                    seqno += 1
                    parser.buf = ""

                # clean up the outbuf. remove entries older than 30 minutes.

                while outbuf:
                    if outbuf[0][0] < t - MAX_MSG_AGE:
                        outbuf.pop(0)
                    else:
                        break

                # send the next message to nanomsg only if the prev got some kind of answer

                if soc_waiting_for_ack is not None:
                    if time.time() - soc_waiting_for_ack > MAX_ACK_TIMEOUT:
                        log.warning("No ack for %d ... reconnecting. (queue %d)", MAX_ACK_TIMEOUT, len(outbuf))
                        soc_waiting_for_ack = None

                        soc.close()
                        soc = connect(server)
                    else:
                        try:
                            if soc.recv(flags=DONTWAIT):
                                soc_waiting_for_ack = None
                                # remove packets for which we just got the ack.
                                outbuf = outbuf[outbuf_tx_index:]
                        except NanoMsgAPIError as e:
                            if e.errno == errno.EAGAIN:
                                pass
                            else:
                                # unknown error!
                                raise

                if soc_waiting_for_ack is None and outbuf:
                    txmsg = "\n".join([e[1] for e in outbuf])  # join all messages to one big.
                    outbuf_tx_index = len(outbuf)

                    soc.send(txmsg)
                    soc_waiting_for_ack = time.time()

                time.sleep(.01)
        except serial.SerialException as e:
            log.warning("Serial port disconnected: %s. Will try to open again." % (e.message))


def main():
    from argparse import ArgumentParser
    ap = ArgumentParser(description="Printf UART logger that logs to nanomsg")
    ap.add_argument("server", help="nanomsg printf server, for example tcp://logserver.local:14999")
    ap.add_argument("port", help="Serial port")
    ap.add_argument("baud", default=115200, help="Serial port baudrate")
    ap.add_argument("--portname", default=None)
    ap.add_argument("--no-mts", default=False, action="store_true")
    ap.add_argument("--debug", default=False, action="store_true")
    ap.add_argument("--binary", default="", help="Binary mode file")
    args = ap.parse_args()

    if args.debug:
        loglevel = logging.NOTSET
    else:
        loglevel = logging.INFO

    logging.basicConfig(level=loglevel, format="%(asctime)s %(name)s %(levelname)-5s: %(message)s")
    run(args.server, args.port, args.baud, args.portname, not args.no_mts, args.debug, args.binary)


if __name__ == "__main__":
    main()
