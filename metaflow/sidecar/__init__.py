from .sidecar import Sidecar
from .sidecar_messages import MessageTypes, Message
from .sidecar_subprocess import SidecarSubProcess


class DummyFlow(object):
    def __init__(self):
        self.name = "not_a_real_flow"
