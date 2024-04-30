import sys
from contextlib import contextmanager
from typing import Optional, Union


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
    def environment(self) -> Optional["metaflow_environment.MetaflowEnvironment"]:
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

    @property
    def flow(
        self,
    ) -> Optional[Union["metaflow.flowspec.FlowSpec", "metaflow.sidecar.DummyFlow"]]:
        from metaflow.sidecar import DummyFlow

        if self._flow is None:
            self._flow = DummyFlow()
            self._flow_name = self._flow.name
        return self._flow

    @property
    def flow_name(self) -> Optional[str]:
        return self._flow_name

    @property
    def monitor(self) -> Optional["metaflow.monitor.NullMonitor"]:
        from .plugins import MONITOR_SIDECARS
        from .metaflow_config import DEFAULT_MONITOR

        if self._monitor is None:
            self._monitor = MONITOR_SIDECARS[DEFAULT_MONITOR](
                flow=self.flow, env=self.environment
            )
            self._debug("Started monitor outside of a flow")
            self._monitor.start()
        return self._monitor

    def set_monitor(
        self,
        flow: "metaflow.flowspec.FlowSpec",
        environment: "metaflow_environment.MetaflowEnvironment",
        monitor: "metaflow.monitor.NullMonitor",
    ) -> None:
        self._flow = flow
        self._flow_name = flow.name
        self._environment = environment
        self._monitor = monitor

    @staticmethod
    def _debug(msg: str):
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
    def measure(self, name: str):
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
    def count(self, name: str):
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

    def gauge(self, gauge: "metaflow.monitor.Gauge"):
        """
        Log a gauge.

        Parameters
        ----------
        name : str
            The name of the gauge.
        value : float
            The value of the gauge.

        """
        self.monitor.gauge(gauge)


_system_monitor = SystemMonitor()
