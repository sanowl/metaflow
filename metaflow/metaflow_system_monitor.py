import sys
from contextlib import contextmanager


class SystemMonitor(object):
    def __init__(self):
        self._flow = None
        self._environment = None
        self._monitor = None
        self._flow_name = None

    def __del__(self):
        if self.flow_name == "not_a_real_flow":
            self.monitor.terminate()

    @property
    def environment(self):
        from .plugins import ENVIRONMENTS
        from .metaflow_config import DEFAULT_ENVIRONMENT
        from .metaflow_environment import MetaflowEnvironment

        if self._environment is None:
            self._environment = [
                e
                for e in ENVIRONMENTS + [MetaflowEnvironment]
                if e.TYPE == DEFAULT_ENVIRONMENT
            ][0](self.flow)
        return self._environment

    @environment.setter
    def environment(self, environment):
        self._environment = environment

    @property
    def flow(self):
        from metaflow.sidecar import DummyFlow

        if self._flow is None:
            self._flow = DummyFlow()
            self.flow_name = "not_a_real_flow"
        return self._flow

    @flow.setter
    def flow(self, flow):
        self._flow = flow

    @property
    def flow_name(self):
        return self._flow_name

    @flow_name.setter
    def flow_name(self, flow_name):
        self._flow_name = flow_name

    @property
    def monitor(self):
        from .plugins import MONITOR_SIDECARS
        from .metaflow_config import DEFAULT_MONITOR

        if self._monitor is None:
            self._monitor = MONITOR_SIDECARS[DEFAULT_MONITOR](
                flow=self.flow, env=self.environment
            )
            self._debug("Started monitor outside of a flow")
            self._monitor.start()
        return self._monitor

    @monitor.setter
    def monitor(self, monitor):
        self._monitor = monitor

    def set_monitor(self, flow, environment, monitor):
        self.flow = flow
        self.environment = environment
        self.monitor = monitor

    @staticmethod
    def _debug(msg: str) -> None:
        """
        Log a debug message to stderr.

        Parameters
        ----------
        msg : str
            Message to log.

        Returns
        -------
        None
        """
        print("system monitor: %s" % msg, file=sys.stderr)

    @contextmanager
    def measure(self, name):
        """
        Context manager to measure the execution duration and counter of a block of code.

        Parameters
        ----------
        name : str
            The name to associate with the timer and counter.

        Yields
        ------
        None
        """
        # Delegating the context management to the monitor's measure method
        with self.monitor.measure(name):
            yield

    @contextmanager
    def count(self, name):
        """
        Context manager to increment a counter.

        Parameters
        ----------
        name : str
            The name of the counter.

        Yields
        ------
        None
        """
        # Delegating the context management to the monitor's count method
        with self.monitor.count(name):
            yield

    def gauge(self, name, value):
        """
        Log a gauge.

        Parameters
        ----------
        name : str
            The name of the gauge.
        value : float
            The value of the gauge.

        """
        from .monitor import Gauge

        gauge = Gauge(name)
        gauge.set_value(value)
        self.monitor.gauge(gauge)


_system_monitor = SystemMonitor()
