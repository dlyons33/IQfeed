class BarConn(FeedConn):
    """
    Let's you get live data as interval bar data.
    If you are using interval bars for trading, use this class if you want
    IQFeed to calculate the interval bars for you and send you interval bars
    instead of (or in addition to) receiving every tick. For example, you may
    want to get open, high, low, close data for each minute or every 50 trades.
    The length of the interval can be in time units, number of trades units
    or volume traded units as for bars from HistoryConn.
    If you want historical bars, use HistoryConn instead. This class
    allows you to get some history, for example if you want the past 5
    days's bars to fill in a data structure before you start getting live
    data updates. But if you want historical data for back-testing or some
    such, you are better off using HistoryConn instead.
    Since most historical data that IQFeed gives you is bar data, if you are
    just getting started, it may be a good idea to save some live tick-data and
    bar-data and compare them so you understand exactly how IQFeed is
    filtering ticks and generating it's bars. Different data providers tend to
    do this differently, dome better than others and the documentation usually
    doesn't get updated when things are changed.
    For more info, see:
    www.iqfeed.net/dev/api/docs/Derivatives_Overview.cfm
    and
    www.iqfeed.net/dev/api/docs/Derivatives_StreamingIntervalBars_TCPIP.cfm
    """
    host = FeedConn.host
    port = FeedConn.deriv_port

    interval_data_type = np.dtype(
            [('symbol', 'S64'), ('date', 'M8[D]'), ('time', 'u8'),
             ('open_p', 'f8'), ('high_p', 'f8'), ('low_p', 'f8'),
             ('close_p', 'f8'), ('tot_vlm', 'u8'), ('prd_vlm', 'u8'),
             ('num_trds', 'u8')])

    def __init__(self, name: str = "BarConn", host: str = host,
                 port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()
        self._empty_interval_msg = np.zeros(1, dtype=BarConn.interval_data_type)

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['n'] = self._process_invalid_symbol
        self._pf_dict['B'] = self._process_bars
        self._sm_dict["REPLACED PREVIOUS WATCH"] = self._process_replaced_watch
        self._sm_dict[
            "SYMBOL LIMIT REACHED"] = self._process_symbol_limit_reached
        self._sm_dict["WATCHES"] = self._process_watch

    def _process_invalid_symbol(self, fields: Sequence[str]) -> None:
        """Called when a request is made with an invalid symbol."""
        assert len(fields) > 1
        assert fields[0] == 'n'
        bad_sym = fields[1]
        for listener in self._listeners:
            listener.process_invalid_symbol(bad_sym)

    def _process_replaced_watch(self, fields: Sequence[str]):
        """Called when a request supersedes an prior interval request."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'REPLACED PREVIOUS WATCH'
        symbol = fields[2]
        for listener in self._listeners:
            listener.process_replaced_previous_watch(symbol)

    def _process_symbol_limit_reached(self, fields: Sequence[str]) -> None:
        """Handle IQFeed telling us the symbol limit has been reached."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == "SYMBOL LIMIT REACHED"
        sym = fields[2]
        for listener in self._listeners:
            listener.process_symbol_limit_reached(sym)

    def _process_watch(self, fields: Sequence[str]) -> None:
        """Process a watches message."""
        assert len(fields) > 3
        assert fields[0] == 'S'
        assert fields[1] == 'WATCHES'
        symbol = fields[2]
        interval = fields[3]
        request_id = ""
        if len(fields) > 4:
            request_id = fields[4]
        for listener in self._listeners:
            listener.process_watch(symbol, interval, request_id)

    def _process_bars(self, fields: Sequence[str]):
        """Parse bar data and call appropriate callback."""
        assert len(fields) > 10
        assert fields[0][0] == "B" and fields[1][0] == "B"

        interval_data = self._empty_interval_msg
        interval_data['symbol'] = fields[2]
        interval_data['date'], interval_data['time'] = fr.read_posix_ts(
                fields[3])
        interval_data['open_p'] = np.float64(fields[4])
        interval_data['high_p'] = np.float64(fields[5])
        interval_data['low_p'] = np.float64(fields[6])
        interval_data['close_p'] = np.float64(fields[7])
        interval_data['tot_vlm'] = np.float64(fields[8])
        interval_data['prd_vlm'] = np.float64(fields[9])
        interval_data['num_trds'] = (
            np.float64(fields[10]) if fields[10] != "" else 0)

        bar_type = fields[1][1]
        if bar_type == 'U':
            for listener in self._listeners:
                listener.process_latest_bar_update(interval_data)
        elif bar_type == 'C':
            for listener in self._listeners:
                listener.process_live_bar(interval_data)
        elif bar_type == 'H':
            for listener in self._listeners:
                listener.process_history_bar(interval_data)
        else:
            raise UnexpectedField("Bad bar type in BarConn")

    def watch(self, symbol: str, interval_len: int, interval_type: str = None,
              bgn_flt: datetime.time = None, end_flt: datetime.time = None,
              update: int = None, bgn_bars: datetime.datetime = None,
              lookback_days: int = None, lookback_bars: int = None) -> None:
        """
        Request live interval (bar) data.
        :param symbol: Symbol for which you are requesting data.
        :param interval_len: Interval length in interval_type units
        :param interval_type: 's' = secs, 'v' = volume, 't' = ticks
        :param bgn_flt: Earliest time of day for which you want data
        :param end_flt: Latest time of day for which you want data
        :param update: Update the current bar every update secs.
        :param bgn_bars: Get back-fill bars starting at bgn_bars
        :param lookback_days: Get lookback_days of backfill data
        :param lookback_bars: Get lookback_bars of backfill data
        Only one of bgn_bars, lookback_days or lookback_bars should be set.
        Requests live interval data. You can also request some backfill data.
        When you call this function:
            1) The callback process_history_bar is called for backfill bars
            that go back either a) upto bgn_bars, b) upto lookback_days or
            c) upto lookback_bars.
            2) The callback process_latest_bar_update is called on every update
            to data for the current live bar.
            3) The callback process_live_bar is called every time we cross an
            interval boundary with data for the now complete bar.
        BW,[Symbol],[Interval],[BeginDate BeginTime],[MaxDaysOfDatapoints],
              [MaxDatapoints],[BeginFilterTime],[EndFilterTime],[RequestID],
              [Interval Type],[Reserved],[UpdateInterval]
        """
        assert interval_type in ('s', 'v', 't')
        bgn_bar_set = int(bgn_bars is not None)
        lookback_days_set = int(lookback_days is not None)
        lookback_bars_set = int(lookback_bars is not None)
        assert (bgn_bar_set + lookback_days_set + lookback_bars_set) < 2

        bgn_bar_str = fr.datetime_to_yyyymmdd_hhmmss(bgn_bars)
        lookback_days_str = fr.blob_to_str(lookback_days)
        lookback_bars_str = fr.blob_to_str(lookback_bars)
        bf_str = fr.time_to_hhmmss(bgn_flt)
        ef_str = fr.time_to_hhmmss(end_flt)
        update_str = fr.blob_to_str(update)

        request_id = "B-%s-%0.4d-%s" % (symbol, interval_len, interval_type)

        bar_cmd = "BW,%s,%s,%s,%s,%s,%s,%s,%s,%s,'',%s\r\n" % (
            symbol, interval_len, bgn_bar_str, lookback_days_str,
            lookback_bars_str,
            bf_str, ef_str, request_id, interval_type, update_str)
        self._send_cmd(bar_cmd)

    def unwatch(self, symbol: str):
        """Unwatch a specific symbol"""
        self._send_cmd("BR,%s" % symbol)

    def unwatch_all(self) -> None:
        """Unwatch all symbols."""
        self._send_cmd("S,UNWATCH ALL")

    def request_watches(self) -> None:
        """Request a list of all symbols we have subscribed."""
        self._send_cmd("S,REQUEST WATCHES\r\n")