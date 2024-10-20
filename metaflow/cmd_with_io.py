import subprocess
from .exception import ExternalCommandFailed

from metaflow.util import to_bytes
from security import safe_command


def cmd(cmdline, input, output):
    for path, data in input.items():
        with open(path, "wb") as f:
            f.write(to_bytes(data))

    if safe_command.run(subprocess.call, cmdline, shell=True):
        raise ExternalCommandFailed(
            "Command '%s' returned a non-zero " "exit code." % cmdline
        )

    out = []
    for path in output:
        with open(path, "rb") as f:
            out.append(f.read())

    if len(out) == 1:
        return out[0]
    else:
        return out
