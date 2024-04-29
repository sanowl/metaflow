import sys
from typing import Dict, Any


class SystemLogger(object):
    def __init__(self):
        self._flow = None
        self._environment = None
        self._logger = None
        self._flow_name = None

    def __del__(self):
        if self.flow_name == "not_a_real_flow":
            self.logger.terminate()

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
    def logger(self):
        from .plugins import LOGGING_SIDECARS
        from .metaflow_config import DEFAULT_EVENT_LOGGER

        if self._logger is None:
            self._logger = LOGGING_SIDECARS[DEFAULT_EVENT_LOGGER](
                flow=self.flow, env=self.environment
            )
            self._debug("Started logger outside of a flow")
            self._logger.start()
        return self._logger

    @logger.setter
    def logger(self, logger):
        self._logger = logger

    def set_logger(self, flow, environment, logger):
        self.flow = flow
        self.environment = environment
        self.logger = logger

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
        print("system logger: %s" % msg, file=sys.stderr)

    def log(self, payload: Dict[str, Any]) -> None:
        """
        Log a message to the logger.

        Parameters
        ----------
        payload : Dict[str, Any]
            Payload to log.

        Returns
        -------
        None
        """
        self.logger.log(payload)


_system_logger = SystemLogger()
