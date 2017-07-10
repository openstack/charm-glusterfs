# Copyright 2017 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from io import TextIOBase
from typing import List, Any, IO

from result import Ok, Result

__author__ = 'Chris Holcombe <chris.holcombe@canonical.com>'


class ShellScript(object):
    def __init__(self, interpreter: str, comments: List[str],
                 commands: List[str]) -> None:
        """
        A very basic representation of a shell script. There is an interpreter,
        some comments and a list of commands the interpreter to use
        Create a new ShellScript object
        :param interpreter: str The interpreter to use ie /bin/bash etc
        :param comments: List[str] of comments
        :param commands: List[str] of commands
        """
        self.interpreter = interpreter
        # Any comments here will be joined with newlines when written back out
        self.comments = comments
        # Any commands here will be joined with newlines when written back out
        self.commands = commands

    def write(self, f: TextIOBase) -> Result:
        # Write the run control class back out to a file
        bytes_written = 0
        bytes_written += f.write("{}\n".format(self.interpreter))
        bytes_written += f.write("\n".join(self.comments))
        bytes_written += f.write("\n")
        bytes_written += f.write("\n".join(self.commands))
        bytes_written += f.write("\n")
        return Ok(bytes_written)


def parse(f: IO[Any]) -> Result:
    """
    Parse a shellscript and return a ShellScript
    :param f: TextIOBase handle to the shellscript file
    :return: Result with Ok or Err
    """
    comments = []
    commands = []
    interpreter = ""

    buf = f.readlines()

    for line in buf:
        trimmed = line.strip()
        if trimmed.startswith("#!"):
            interpreter = trimmed
        elif trimmed.startswith("#"):
            comments.append(str(trimmed))
        else:
            # Skip blank lines
            if trimmed:
                commands.append(str(trimmed))
    return Ok(ShellScript(interpreter=interpreter,
                          comments=comments,
                          commands=commands))
