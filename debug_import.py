import importlib
import traceback

try:
    mod = importlib.import_module('pybibliometric_analysis')
    print('package file:', getattr(mod, '__file__', None))
    keys = sorted(list(mod.__dict__.keys()))
    print('dict keys count:', len(keys))
    print('some keys:', keys[:20])
    print('has io_utils attr:', 'io_utils' in mod.__dict__)
    try:
        from pybibliometric_analysis import io_utils
        print('imported io_utils ok')
    except Exception as e:
        print('import error while importing io_utils:', type(e).__name__, e)
except Exception:
    print('import failed:')
    traceback.print_exc()
