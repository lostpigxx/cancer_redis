# cluster_pytest_console.py

import sys
import traceback
import types


class _Skip(Exception):
    pass


class _XFail(Exception):
    pass


class _Mark:
    def parametrize(self, argname, values):
        def decorate(func):
            func.__cluster_parametrize__ = (argname, list(values))
            return func

        return decorate


def _iter_tests(module):
    for name, obj in module.__dict__.items():
        if name.startswith("test_") and callable(obj):
            yield name, obj


def _run_one(name, func, args):
    label = name
    if args:
        label = "{}[{}]".format(name, ",".join(str(arg) for arg in args))

    print("RUN {}".format(label))

    try:
        func(*args)
    except _Skip as exc:
        print("SKIP {}: {}".format(label, exc))
        return 0
    except _XFail as exc:
        print("XFAIL {}: {}".format(label, exc))
        return 0
    except Exception:
        print("FAIL {}".format(label))
        traceback.print_exc()
        return 1

    print("PASS {}".format(label))
    return 0


def _main(args=None):
    module = sys.modules.get("__main__")
    if module is None:
        print("FAIL: cannot locate __main__ module")
        return 1

    tests = list(_iter_tests(module))
    if not tests:
        print("FAIL: no test functions found")
        return 1

    failures = 0
    for name, func in tests:
        parametrize = getattr(func, "__cluster_parametrize__", None)
        if parametrize is None:
            failures += _run_one(name, func, ())
            continue

        _argname, values = parametrize
        for value in values:
            failures += _run_one(name, func, (value,))

    return 1 if failures else 0


def _skip(reason):
    raise _Skip(reason)


def _xfail(reason):
    raise _XFail(reason)


def install():
    module = types.ModuleType("pytest")
    module.main = _main
    module.skip = _skip
    module.xfail = _xfail
    module.mark = _Mark()
    sys.modules["pytest"] = module

