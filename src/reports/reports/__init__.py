import importlib
import pkgutil
from pathlib import Path

pkg_path = Path(__file__).resolve().parent
pkg_name = __name__

for m in pkgutil.iter_modules([str(pkg_path)]):
    if m.name == "__init__":
        continue
    importlib.import_module(f"{pkg_name}.{m.name}")
