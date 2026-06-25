# dsupdt

Python project to archive and update operational datasets for the
[NSF NCAR Geoscience Data Exchange (GDEX)](https://gdex.ucar.edu).

The user guide for this utility tool can be viewed at:
[User guide](https://gdex-docs-dsupdt.readthedocs.io).

## Programs

The package installs the following command-line utility, running as the
current user:

- `dsupdt` — archive and update RDA operational datasets

Run with `--help` (or `-h`) for full usage details.

## Environment setup

Create a Python environment first; package installs in the next section run
inside whichever environment you activate here.

### Option A — Python venv (DECS machines)

```bash
python3 -m venv $ENVHOME          # e.g. /glade/u/home/gdexdata/gdexmsenv
source $ENVHOME/bin/activate
```

### Option B — Conda (DAV/Casper)

```bash
conda create --prefix $ENVHOME python=3.12   # e.g. /glade/work/gdexdata/conda-envs/pg-gdex
conda activate $ENVHOME
```

## Installing rda-python-dsupdt

Pick whichever install mode fits your workflow.  All variants pull in the
transitive dependencies (`rda_python_common`, `rda_python_dsarch`)
automatically.

For local development, clone this repo alongside your project and install it
in editable mode so that changes are picked up without re-installing:

```bash
git clone https://github.com/NCAR/rda-python-dsupdt.git
cd rda-python-dsupdt
pip install -e .
```

To test a specific branch (e.g. an in-progress feature or fix branch), pass
`-b/--branch` to `git clone`:

```bash
git clone -b <branch-name> https://github.com/NCAR/rda-python-dsupdt.git
cd rda-python-dsupdt
pip install -e .
```

For a regular (non-editable) install from a checkout:

```bash
pip install /path/to/rda-python-dsupdt
```

For a production install on a system that uses the published distribution:

```bash
pip install rda_python_dsupdt
```

## Documentation sync

The user guide at
[gdex-docs-dsupdt.readthedocs.io](https://gdex-docs-dsupdt.readthedocs.io) is
generated from `src/rda_python_dsupdt/dsupdt.usg`.  Keep all user-facing content
in `dsupdt.usg` — no manual RST editing is required.

When a pull request modifying `dsupdt.usg` is opened, an automated workflow
converts it into RST source files and the version number from this repository's
`pyproject.toml` into the
[gdex-docs-dsupdt](https://github.com/NCAR/gdex-docs-dsupdt) repository, then
opens a pull request from `automated-update-branch` against its `main` branch for
review.

To publish to Read the Docs:

- **Merge** that pull request into `main` to serve the content as the `latest`
  version.
- **Create a GitHub release** in `gdex-docs-dsupdt` to serve the latest release
  as the `stable` version.
