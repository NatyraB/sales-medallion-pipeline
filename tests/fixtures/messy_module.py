# a deliberately messy module used as a problematic fixture
from os.path import *  # wildcard import
import json  # unused import


def go():
    try:
        risky()  # noqa - undefined on purpose
    except:  # bare except
        pass
    print("done")  # TODO: switch to logging
