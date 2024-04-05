from typing import Any, Optional


class SystemCurrent(object):
    def __init__(self):
        self._metrics_manager = None

    def __contains__(self, key: str):
        return getattr(self, key, None) is not None

    def get(self, key: str, default=None) -> Optional[Any]:
        return getattr(self, key, default)

    def _set_env(self, metrics_manager=None):
        if metrics_manager is not None:
            self._metrics_manager = metrics_manager

    def _update_env(self, env):
        for k, v in env.items():
            setattr(self.__class__, k, property(fget=lambda _, x=v: v))

    @property
    def monitor(self):
        return self.metrics_manager.monitor

    @property
    def logger(self):
        return self.metrics_manager.logger

    @property
    def metrics_manager(self):
        # This is a lazy initialization of the metrics manager to avoid unnecessary computation
        # _metrics_manager is None when the system is not running a flow
        if self._metrics_manager is None:
            from metaflow.metaflow_metrics_manager import MetricsManager
            self._metrics_manager = MetricsManager()
            self._metrics_manager.start()
            return self._metrics_manager
        return self._metrics_manager


system_current = SystemCurrent()