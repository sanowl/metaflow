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

    def get_counter_payload(self, payload):
        if self._sidecar.is_active and payload:
            msg = Message(MessageTypes.BEST_EFFORT, payload.get("counter"))
            return msg

    def get_measure_payload(self, payload):
        if self._sidecar.is_active and payload:
            counter_msg = Message(MessageTypes.BEST_EFFORT, payload.get("counter"))
            timer_msg = Message(MessageTypes.BEST_EFFORT, payload.get("timer"))
            return counter_msg, timer_msg

    def log_event(self, msg):
        if self._sidecar.is_active and msg:
            self._sidecar.send(msg)

    @classmethod
    def get_worker(cls):
        return None
