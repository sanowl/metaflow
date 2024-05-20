from metaflow.sidecar import Message, MessageTypes, Sidecar


class NullEventLogger(object):
    TYPE = "nullSidecarLogger"

    def __init__(self, *args, **kwargs):
        # Currently passed flow and env in kwargs
        self._sidecar = Sidecar(self.TYPE)

    def start(self):
        return self._sidecar.start()

    def terminate(self):
        return self._sidecar.terminate()

    def send(self, msg):
        # Arbitrary message sending. Useful if you want to override some different
        # types of messages.
        self._sidecar.send(msg)

    def log(self, payload):
        if self._sidecar.is_active:
            msg = Message(MessageTypes.BEST_EFFORT, payload)
            self._sidecar.send(msg)

    def log_event(self, msg=None, event_name=None, log_stream=None, other_context=None):
        """
        Log an event to the event logger.

        Parameters
        ----------
        msg : str
            Message to log.
        event_name : str
            Name of the event to log. Used for grouping similar event types.
        log_stream : str
            Name of the log stream to log to. Used for grouping events by log stream.
        other_context : dict
            Additional context to log with the event. The additional context will have to be handled by
            the event logger implementation.
        """
        if self._sidecar.is_active:
            payload = {
                "msg": msg,
                "event_name": event_name,
                "log_stream": log_stream,
                "other_context": other_context or {},
            }
            self.log(payload)

    @classmethod
    def get_worker(cls):
        return None
