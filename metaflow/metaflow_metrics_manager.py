import sys
from contextlib import contextmanager
from .metaflow_environment import MetaflowEnvironment
from .metaflow_config import DEFAULT_EVENT_LOGGER, DEFAULT_MONITOR, DEFAULT_ENVIRONMENT
from .plugins import MONITOR_SIDECARS, LOGGING_SIDECARS, ENVIRONMENTS


class DummyFlow(object):
    def __init__(self):
        self.name = "not_a_real_flow"


class MetricsManager(object):
    def __init__(self, flow=DummyFlow(), monitor=None, logger=None, environment=None):
        self._flow_name = flow.name
        if environment is None:
            environment = [
                e for e in ENVIRONMENTS + [MetaflowEnvironment] if e.TYPE == DEFAULT_ENVIRONMENT
            ][0](flow)

        if monitor is None:
            monitor = MONITOR_SIDECARS[DEFAULT_MONITOR](flow=flow, env=environment)

        if logger is None:
            logger = LOGGING_SIDECARS[DEFAULT_EVENT_LOGGER](flow=flow, env=environment)

        self._monitor = monitor
        self._logger = logger
        self._environment = environment

    def __del__(self):
        if self.flow_name == "not_a_real_flow":
            self.monitor.terminate()
            self.logger.terminate()

    def start(self):
        self._debug("Started monitor and loggers outside of flow")
        self._monitor.start()
        self._logger.start()

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
        print("metrics manager: %s" % msg, file=sys.stderr)

    @property
    def monitor(self):
        return self._monitor

    @property
    def logger(self):
        return self._logger

    @property
    def environment(self):
        return self._environment

    @property
    def flow_name(self):
        return self._flow_name

    @contextmanager
    def count(self, metric_name, qualifer_name=None):
        monitor_msg = self.monitor.get_count_payload(metric_name)
        logger_msg = self.logger.get_count_payload(monitor_msg.payload, qualifer_name)
        yield
        self.monitor.send_metric(monitor_msg)
        self.logger.log_event(logger_msg)

    @contextmanager
    def measure(self, metric_name, qualifer_name=None):
        monitor_msg = self.monitor.get_measure_payload(metric_name)
        counter_msg, timer_msg = self.logger.get_measure_payload(monitor_msg.payload, qualifer_name)
        yield
        self.monitor.send_metric(monitor_msg)
        self.logger.log_event(counter_msg)
        self.logger.log_event(timer_msg)
