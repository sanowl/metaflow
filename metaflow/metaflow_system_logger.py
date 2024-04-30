import sys
from typing import Dict, Any, Optional, Union


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
    def logger(self) -> Optional["metaflow.event_logger.NullEventLogger"]:
        from .plugins import LOGGING_SIDECARS
        from .metaflow_config import DEFAULT_EVENT_LOGGER

        if self._logger is None:
            self._logger = LOGGING_SIDECARS[DEFAULT_EVENT_LOGGER](
                flow=self.flow, env=self.environment
            )
            self._debug("Started logger outside of a flow")
            self._logger.start()
        return self._logger

    def set_logger(
        self,
        flow: "metaflow.flowspec.FlowSpec",
        environment: "metaflow_environment.MetaflowEnvironment",
        logger: "metaflow.event_logger.NullEventLogger",
    ):
        self._flow = flow
        self._flow_name = flow.name
        self._environment = environment
        self._logger = logger

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
        print("system logger: %s" % msg, file=sys.stderr)

    def log(self, payload: Dict[str, Any]):
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
