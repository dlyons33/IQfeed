class FeedConn:
    """
    FeedConn is the base class for other XXXConn classes
    It handles connecting, disconnecting, sending messages to IQFeed,
    reading responses from IQFeed, feed status messages etc.
    """

    protocol = "6.0"

    iqfeed_host = os.getenv('IQFEED_HOST', "127.0.0.1")
    quote_port = int(os.getenv('IQFEED_PORT_QUOTE', 5009))
    lookup_port = int(os.getenv('IQFEED_PORT_LOOKUP', 9100))
    depth_port = int(os.getenv('IQFEED_PORT_DEPTH', 9200))
    admin_port = int(os.getenv('IQFEED_PORT_ADMIN', 9300))
    deriv_port = int(os.getenv('IQFEED_PORT_DERIV', 9400))

    host = iqfeed_host
    port = quote_port

    databuf = namedtuple(
        "databuf", ('failed', 'err_msg', 'num_pts', 'raw_data'))

    def __init__(self, name: str, host: str, port: int):
        self._host = host
        self._port = port
        self._name = name

        self._stop = threading.Event()
        self._start_lock = threading.Lock()
        self._connected = False
        self._reconnect_failed = False
        self._pf_dict = {}
        self._sm_dict = {}
        self._listeners = []
        self._buf_lock = threading.RLock()
        self._send_lock = threading.RLock()
        self._recv_buf = ""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._read_thread = threading.Thread(group=None, target=self,
                                             name="%s-reader" % self._name,
                                             args=(), kwargs={}, daemon=None)
        self._set_message_mappings()

    def connect(self) -> None:
        """
        Connect to the appropriate socket and start the reading thread.
        You must call this before you start using an XXXConn class. If
        this thread is not running, no callbacks will be called, no data
        will be returned by functions which return data immediately.
        """
        self._sock.connect((self._host, self._port))
        self._set_protocol(FeedConn.protocol)
        self._set_client_name(self.name())
        self._send_connect_message()
        self.start_runner()

    def start_runner(self) -> None:
        """Called to start the reading thread."""
        with self._start_lock:
            self._stop.clear()
            if not self.reader_running():
                self._read_thread.start()

    def disconnect(self) -> None:
        """
        Stop the reading thread and disconnect from the socket to IQFeed.exe
        Call this to ensure sockets are closed and we exit cleanly.
        """
        self.stop_runner()
        if self._sock:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            self._sock = None

    def stop_runner(self) -> None:
        """Called to stop the reading and message processing thread."""
        with self._start_lock:
            self._stop.set()
            if self.reader_running():
                self._read_thread.join(30)

    def reader_running(self) -> bool:
        """
        True if the reader thread is running.
        If you don't get updates for a while you "may" want to query this
        function.  Mainly useful for debugging during development of the
        library.  If the reader thread is crashing, there is likely a bug
        in the library or something else is going very wrong.
        """
        return self._read_thread.is_alive()

    def connected(self) -> bool:
        """
        Returns true if IQClient.exe is connected to DTN's servers.
        It may take a few seconds after connecting to IQFeed for IQFeed to tell
        us it is connected to DTN's servers. During these few seconds, this
        function will return False even though it's not actually a problem.
        NOTE: It's not telling you if you are connected to IQFeed.exe. It's
        telling you if IQFeed.exe is connected to DTN's servers.
        """
        return self._connected

    def name(self) -> str:
        """Return whatever you named this conn class in the constructor"""
        return self._name

    def _send_cmd(self, cmd: str) -> None:
        with self._send_lock:
            self._sock.sendall(cmd.encode(encoding='latin-1'))

    def reconnect_failed(self) -> bool:
        """
        Returns true if IQClient.exe failed to reconnect to DTN's servers.
        It can and does happen that IQClient.exe drops a connection to DTN's
        servers and then reconnects. This is not a big problem. But if a
        reconnect fails this means there is a big problem and you should
        probably pause trading and figure out what's going on with your
        network.
        """
        return self._reconnect_failed

    def __call__(self):
        """The reader thread runs this in a loop."""
        while not self._stop.is_set():
            if self._read_messages():
                self._process_messages()

    def _read_messages(self) -> bool:
        """Read raw text sent by IQFeed on socket"""
        ready_list = select.select([self._sock], [], [self._sock], 5)
        if ready_list[2]:
            raise RuntimeError(
                    "Error condition on socket connection to IQFeed: %s,"
                    "" % self.name())
        if ready_list[0]:
            data_recvd = self._sock.recv(1024).decode('latin-1')
            with self._buf_lock:
                self._recv_buf += data_recvd
                return True
        return False

    def _next_message(self) -> str:
        """Next complete message from buffer of delimited messages"""
        with self._buf_lock:
            next_delim = self._recv_buf.find('\n')
            if next_delim != -1:
                message = self._recv_buf[:next_delim].strip()
                self._recv_buf = self._recv_buf[(next_delim + 1):]
                return message
            else:
                return ""

    def _set_message_mappings(self) -> None:
        """Creates map of message names to processing functions."""
        self._pf_dict['E'] = self._process_error
        self._pf_dict['T'] = self._process_timestamp
        self._pf_dict['S'] = self._process_system_message

        self._sm_dict["SERVER DISCONNECTED"] = \
            self._process_server_disconnected
        self._sm_dict["SERVER CONNECTED"] = self._process_server_connected
        self._sm_dict[
            "SERVER RECONNECT FAILED"] = self._process_reconnect_failed
        self._sm_dict["CURRENT PROTOCOL"] = self._process_current_protocol
        self._sm_dict["STATS"] = self._process_conn_stats

    def _process_messages(self) -> None:
        """Process the next complete message waiting to be processed"""
        message = self._next_message()
        while "" != message:
            fields = message.split(',')
            handle_func = self._processing_function(fields)
            handle_func(fields)
            message = self._next_message()

    def _processing_function(self, fields):
        """Returns the processing function for this specific message."""
        pf = self._pf_dict.get(fields[0][0])
        if pf is not None:
            return pf
        else:
            return self._process_unregistered_message

    def _process_unregistered_message(self, fields: Sequence[str]) -> None:
        """Called if we get a message we don't expect.
        Appropriate action here is probably to crash.
        """
        err_msg = ("Unexpected message received by %s: %s" % (
            self.name(), ",".join(fields)))
        raise UnexpectedMessage(err_msg)

    def _process_system_message(self, fields: Sequence[str]) -> None:
        """
        Called when the next message is a system message.
        System messages are messages about the state of the data delivery
        system, including IQConnect.exe, DTN servers and connectivity.
        """
        assert len(fields) > 1
        assert fields[0] == "S"
        processing_func = self._system_processing_function(fields)
        processing_func(fields)

    def _system_processing_function(self, fields):
        """Returns the appropriate system message handling function."""
        assert len(fields) > 1
        assert fields[0] == "S"
        spf = self._sm_dict.get(fields[1])
        if spf is not None:
            return spf
        else:
            return self._process_unregistered_system_message

    def _process_unregistered_system_message(self,
                                             fields: Sequence[str]) -> None:
        """
        Called if we get a system message we don't know how to handle.
        Appropriate action here is probably to crash.
        """
        err_msg = ("Unexpected message received by %s: %s" % (
            self.name(), ",".join(fields)))
        raise UnexpectedMessage(err_msg)

    def _process_current_protocol(self, fields: Sequence[str]) -> None:
        """
        Process the Current Protocol Message
        The first message we send IQFeed.exe upon connecting is the
        set protocol message. If we get this message and the protocol
        IQFeed tells us it's using does not match the expected protocol
        then the we really need to shutdown, fix the version mismatch by
        upgrading/downgrading IQFeed.exe and this library so they match.
        """
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "CURRENT PROTOCOL"
        protocol = fields[2]
        if protocol != FeedConn.protocol:
            err_msg = ("Desired Protocol %s, Server Says Protocol %s in %s" % (
                FeedConn.protocol, protocol, self.name()))
            raise UnexpectedProtocol(err_msg)

    def _process_server_disconnected(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe disconnects from DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER DISCONNECTED"
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()

    def _process_server_connected(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe connects or re-connects to DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER CONNECTED"
        self._connected = True
        for listener in self._listeners:
            listener.feed_is_fresh()

    def _process_reconnect_failed(self, fields: Sequence[str]) -> None:
        """Called if IQFeed.exe cannot reconnect to DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER RECONNECT FAILED"
        self._reconnect_failed = True
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()
            listener.feed_has_error()

    ConnStatsMsg = namedtuple(
        'ConnStatsMsg', (
            'server_ip', 'server_port', 'max_sym', 'num_sym', 'num_clients',
            'secs_since_update', 'num_recon', 'num_fail_recon',
            'conn_tm', 'mkt_tm',
            'status', 'feed_version', 'login',
            'kbs_recv', 'kbps_recv', 'avg_kbps_recv',
            'kbs_sent', 'kbps_sent', 'avg_kbps_sent'))

    def _process_conn_stats(self, fields: Sequence[str]) -> None:
        """Parse and send ConnStatsMsg to listener."""
        assert len(fields) > 20
        assert fields[0] == "S"
        assert fields[1] == "STATS"

        # noinspection PyCallByClass
        conn_stats = FeedConn.ConnStatsMsg(
            server_ip=fields[2],
            server_port=fr.read_int(fields[3]),
            max_sym=fr.read_int(fields[4]),
            num_sym=fr.read_int(fields[5]),
            num_clients=fr.read_int(fields[6]),
            secs_since_update=fr.read_int(fields[7]),
            num_recon=fr.read_int(fields[8]),
            num_fail_recon=fr.read_int(fields[9]),
            conn_tm=(time.strptime(fields[10], "%b %d %I:%M%p")
                     if fields[10] != "" else None),
            mkt_tm=(time.strptime(fields[11], "%b %d %I:%M%p")
                    if self.connected() else None),
            status=(fields[12] == "Connected"),
            feed_version=fields[13],
            login=fields[14],
            kbs_recv=fr.read_float(fields[15]),
            kbps_recv=fr.read_float(fields[16]),
            avg_kbps_recv=fr.read_float(fields[17]),
            kbs_sent=fr.read_float(fields[18]),
            kbps_sent=fr.read_float(fields[19]),
            avg_kbps_sent=fr.read_float(fields[20]))
        for listener in self._listeners:
            listener.process_conn_stats(conn_stats)

    TimeStampMsg = namedtuple("TimeStampMsg", ("date", "time"))

    def _process_timestamp(self, fields: Sequence[str]) -> None:
        """Parse timestamp and send to listener."""
        # T,[YYYYMMDD HH:MM:SS]
        assert fields[0] == "T"
        assert len(fields) > 1
        dt_tm_tuple = fr.read_timestamp_msg(fields[1])
        # noinspection PyCallByClass
        timestamp = FeedConn.TimeStampMsg(date=dt_tm_tuple[0],
                                          time=dt_tm_tuple[1])
        for listener in self._listeners:
            listener.process_timestamp(timestamp)

    def _process_error(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe sends us an error message."""
        assert fields[0] == "E"
        assert len(fields) > 1
        for listener in self._listeners:
            listener.process_error(fields)

    def add_listener(self, listener) -> None:
        """
        Call this to receive updates from this Conn class.
        :param listener: An object of the appropriate listener class.
        You need to call this function with each object that you want messages
        sent to. The object must be of (or derived from) the "appropriate"
        listener class. The various processing functions call callbacks in
        the listeners that have been registered to them when the Conn class
        receives messages.
        """
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener) -> None:
        """
        Call this to unsubscribe the listener object.
        :param listener: An object of the appropriate listener class.
        You must call this if a listener class that has been subscribed for
        updates is going to be deleted. Since python is GC'd, if you don't
        do this the object won't actually be destroyed since the Conn class
        holds a handle to it and it will keep sending the object messages.
        You may want to add something that unsubscribes to the listener
        object's destructor.
        """
        if listener in self._listeners:
            self._listeners.remove(listener)

    def _set_protocol(self, protocol) -> None:
        self._send_cmd("S,SET PROTOCOL,%s\r\n" % protocol)

    def _send_connect_message(self) -> None:
        msg = "S,CONNECT\r\n"
        self._send_cmd(msg)

    def _send_disconnect_message(self) -> None:
        self._send_cmd("S,DISCONNECT\r\n")

    def _set_client_name(self, name) -> None:
        self._name = name
        msg = "S,SET CLIENT NAME,%s\r\n" % name
        self._send_cmd(msg)