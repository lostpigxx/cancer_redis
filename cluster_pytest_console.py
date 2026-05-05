# cluster_pytest_console.py

import importlib.abc
import importlib.machinery
import os
import runpy
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
    namespace = module
    if not isinstance(namespace, dict):
        namespace = module.__dict__

    for name, obj in namespace.items():
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


def _run_tests_in_namespace(namespace):
    tests = list(_iter_tests(namespace))
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


def _is_current_main_file(path):
    module = sys.modules.get("__main__")
    if module is None:
        return False

    main_file = getattr(module, "__file__", None)
    if not main_file:
        return False

    return os.path.abspath(main_file) == os.path.abspath(path)


def _run_test_file(path):
    if _is_current_main_file(path):
        module = sys.modules.get("__main__")
        return _run_tests_in_namespace(module)

    namespace = runpy.run_path(path, run_name="__cluster_pytest_file__")
    return _run_tests_in_namespace(namespace)


def _test_file_args(args):
    return [arg for arg in args if arg.endswith(".py")]


def _main(args=None):
    if args is None:
        args = sys.argv[1:]

    test_files = _test_file_args(args)
    if not test_files:
        module = sys.modules.get("__main__")
        if module is None:
            print("FAIL: cannot locate __main__ module")
            return 1

        return _run_tests_in_namespace(module)

    failures = 0
    for path in test_files:
        print("FILE {}".format(path))
        failures += _run_test_file(path)

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
    module.__loader__ = _PytestShimLoader()
    module.__package__ = "pytest"
    module.__path__ = []
    module.__spec__ = importlib.machinery.ModuleSpec(
        "pytest",
        module.__loader__,
        is_package=True,
    )
    module.__spec__.submodule_search_locations = []
    sys.modules["pytest"] = module


class _PytestShimLoader(importlib.abc.Loader):
    def create_module(self, spec):
        return None

    def get_code(self, fullname):
        if fullname == "pytest.__main__":
            return compile(
                "from cluster_pytest_console import _main\n"
                "raise SystemExit(_main())\n",
                "<cluster pytest shim>",
                "exec",
            )

        return None

    def exec_module(self, module):
        module.main = _main
        module.skip = _skip
        module.xfail = _xfail
        module.mark = _Mark()

        if module.__name__ == "pytest.__main__":
            raise SystemExit(_main())


class _PytestShimFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "pytest":
            return importlib.machinery.ModuleSpec(
                fullname,
                _PytestShimLoader(),
                is_package=True,
            )

        if fullname == "pytest.__main__":
            return importlib.machinery.ModuleSpec(
                fullname,
                _PytestShimLoader(),
            )

        return None


def install_import_hook():
    for finder in sys.meta_path:
        if isinstance(finder, _PytestShimFinder):
            return

    sys.meta_path.insert(0, _PytestShimFinder())
