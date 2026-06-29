"""rda_python_dsupdt: dataset update (dsupdt) utility package.

This package exposes two parallel APIs:

1. Legacy module-based API (back-compat). Import the capitalized
   submodule and call its module-level functions, e.g.::

       from rda_python_dsupdt import PgUpdt

2. Class-based API (preferred for new code). Import the class from the
   lower-case module and either instantiate or subclass it, e.g.::

       from rda_python_dsupdt.pg_updt import PgUpdt

The legacy submodule is eagerly imported below so that
``from rda_python_dsupdt import PgUpdt`` continues to return the module
object that existing callers expect.
"""

from . import PgUpdt

__version__ = "2.0.16"

__all__ = [
   "PgUpdt",
   "__version__",
]
