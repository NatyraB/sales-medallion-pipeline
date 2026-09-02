# A fixture that intentionally does NOT parse, to exercise resilience.
# pytest never imports this file (it is not a test module).
def broken(:
    return None
