from typing import Callable
from decimal import Decimal
from pydantic import BaseModel

from time import time, sleep
from json import JSONEncoder

class DACheckResults(BaseModel):
    accessible: bool
    available: bool

class InitException(Exception):
    pass

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        else:
            return super().default(obj)

# Based on
# - https://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds/49801719#49801719
def every(task: Callable, delay: int) -> None:
    first_time = True
    next_time = time() + delay
    while True:
        if not first_time:
            sleep(max(0, next_time - time()))
        else:
            first_time = False
        task()
        next_time += (time() - next_time) // delay * delay + delay