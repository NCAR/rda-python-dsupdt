#!/usr/bin/env python3
##################################################################################
#     Title: _check_config
#    Author: Zaihua Ji, zji@ucar.edu
#   Purpose: read-only audit of all dsupdt update configurations in RDADB,
#            reporting every invalid record instead of aborting on the first
#            one (as the interactive -SC/-SL/-SR/-SA set actions do)
#    Github: https://github.com/NCAR/rda-python-dsupdt.git
##################################################################################

import re
import sys
from . import __version__
from .dsupdt import DsUpdt

# readable names for the temporal pattern unit keys returned by
# temporal_pattern_units (see check_download_workdir in dsupdt.py)
UNIT_NAMES = {'C': 'Century', 'Y': 'Year', 'Q': 'Quarter', 'M': 'Month',
              'D': 'Day', 'H': 'Hour', 'N': 'Minute', 'S': 'Second'}
UNIT_ORDER = ['C', 'Y', 'Q', 'M', 'D', 'H', 'N', 'S']


def frequency_error(app, val):
   """Return an error message for an invalid update frequency, else None.

   Mirrors the 'freq' branch of DsUpdt.check_interval_field using the same
   get_control_frequency() parser (single count with a Y/M/W/D/H/N/S unit, or a
   fraction of a month with denominator 2, 3, 5, 6, or 10).
   """
   if not val or val == '0': return None
   (freq, err) = app.get_control_frequency(val)
   return None if freq else err


def tinterval_error(val):
   """Return an error message for an invalid remote time interval, else None.

   Mirrors the 'tintv' branch of DsUpdt.check_interval_field: a single count
   with a unit of Y, M, W, D, or H (no N/S).
   """
   if not val or val == '0': return None
   if not re.match(r'^\d*[YMWDH]$', val, re.I):
      return "time interval '{}' must be a count with unit (Y,M,W,D,H)".format(val)
   return None


def interval_error(val):
   """Return an error message for an invalid multi-unit interval, else None.

   Mirrors the default branch of DsUpdt.check_interval_field (validint,
   agetime, nextdue, retryint, cntloffset): one or more count/unit pairs from
   (Y,M,W,D,H,N,S), no fraction.
   """
   if not val or val == '0': return None
   if re.search(r'/\d+$', val):
      return "'{}' does NOT support a fraction".format(val)
   if not re.search(r'\d+[YMWDHNS]', val, re.I):
      return "invalid '{}', must be count(s) with unit(s) (Y,M,W,D,H,N,S)".format(val)
   return None


def workdir_missing_units(app, workdir, dlcmd, remotefile, serverfile, rcmd):
   """Return the set of date units missing from the workdir/landing name.

   Mirrors DsUpdt.check_download_workdir: any temporal unit that distinguishes
   source files (serverfile or the effective download command) but is captured
   by neither the landing name (remotefile) nor the working directory would
   cause different-date downloads to overwrite each other.
   """
   dccmd = rcmd if rcmd else dlcmd
   src = app.download_pattern_units(serverfile) | app.download_pattern_units(dccmd)
   if not src: return set()
   cover = app.download_pattern_units(remotefile) | app.download_pattern_units(workdir)
   return src - cover


def iter_rows(res):
   """Yield row dicts from a column-oriented pgmget() result (or nothing)."""
   if not res or isinstance(res, int): return
   cols = list(res.keys())
   for i in range(len(res[cols[0]])):
      yield {c: res[c][i] for c in cols}


def report(table, ident, errs, counts):
   """Print a record's problems (if any) and accumulate per-table error counts."""
   if not errs: return
   counts[table] = counts.get(table, 0) + 1
   print("[{}] {}".format(table, ident))
   for e in errs:
      print("    - " + e)


def check_controls(app, cnd, counts):
   """Audit all dcupdt update control records matching *cnd*."""
   acts = app.PGOPT['UPDTACTS']
   fields = "cindex, dsid, pindex, action, frequency, validint, retryint, cntloffset"
   for rec in iter_rows(app.pgmget("dcupdt", fields, cnd)):
      errs = []
      if rec['pindex'] and not app.pgget("dcupdt", "", "cindex = {}".format(rec['pindex'])):
         errs.append("ParentIndex {} is not in RDADB".format(rec['pindex']))
      if rec['action'] and not re.match(r'^({})$'.format(acts), rec['action']):
         errs.append("Action '{}' is not a dsupdt action ({})".format(rec['action'], acts))
      for (fld, name, err) in (('frequency', 'Frequency', frequency_error(app, rec['frequency'])),
                               ('validint', 'ValidInterval', interval_error(rec['validint'])),
                               ('retryint', 'RetryInterval', interval_error(rec['retryint'])),
                               ('cntloffset', 'ControlOffset', interval_error(rec['cntloffset']))):
         if err: errs.append("{}: {}".format(name, err))
      report("dcupdt", "cindex {} (ds {})".format(rec['cindex'], rec['dsid']), errs, counts)


def check_locfiles(app, cnd, counts):
   """Audit all dlupdt local file records matching *cnd*."""
   acts = app.PGOPT['ARCHACTS']
   fields = "lindex, dsid, cindex, action, frequency, validint, nextdue, agetime"
   for rec in iter_rows(app.pgmget("dlupdt", fields, cnd)):
      errs = []
      if rec['cindex'] and not app.pgget("dcupdt", "", "cindex = {}".format(rec['cindex'])):
         errs.append("ControlIndex {} is not in RDADB".format(rec['cindex']))
      if rec['action'] and not re.match(r'^({})$'.format(acts), rec['action']):
         errs.append("Action '{}' is not a dsarch action ({})".format(rec['action'], acts))
      for (fld, name, err) in (('frequency', 'Frequency', frequency_error(app, rec['frequency'])),
                               ('validint', 'ValidInterval', interval_error(rec['validint'])),
                               ('nextdue', 'DueInterval', interval_error(rec['nextdue'])),
                               ('agetime', 'AgeTime', interval_error(rec['agetime']))):
         if err: errs.append("{}: {}".format(name, err))
      report("dlupdt", "lindex {} (ds {})".format(rec['lindex'], rec['dsid']), errs, counts)


def check_rmtfiles(app, cnd, counts):
   """Audit all drupdt remote file records matching *cnd*."""
   locinfo = {}   # lindex -> (workdir, download) for the workdir collision check
   for rec in iter_rows(app.pgmget("dlupdt", "lindex, workdir, download", cnd)):
      locinfo[rec['lindex']] = (rec['workdir'], rec['download'])
   fields = "lindex, dsid, remotefile, serverfile, download, tinterval, begintime, endtime"
   for rec in iter_rows(app.pgmget("drupdt", fields, cnd)):
      errs = []
      if not app.pgget("dlupdt", "", "lindex = {}".format(rec['lindex'])):
         errs.append("LocalIndex {} is not in RDADB".format(rec['lindex']))
      err = tinterval_error(rec['tinterval'])
      if err: errs.append("TimeInterval: " + err)
      if not rec['tinterval'] and (rec['begintime'] or rec['endtime']):
         errs.append("BeginTime/EndTime are set without a TimeInterval (ignored at update time)")
      (workdir, dlcmd) = locinfo.get(rec['lindex'], (None, None))
      miss = workdir_missing_units(app, workdir, dlcmd, rec['remotefile'], rec['serverfile'], rec['download'])
      if miss:
         mstr = ', '.join(UNIT_NAMES[u] for u in UNIT_ORDER if u in miss)
         errs.append("WorkDir '{}' MISS temporal pattern(s) of {}; downloaded files overwrite each other before archiving".format(workdir if workdir else '', mstr))
      report("drupdt", "lindex {} / {} (ds {})".format(rec['lindex'], rec['remotefile'], rec['dsid']), errs, counts)


def main():
   """Scan dsupdt configurations and print invalid records; exit 1 if any found."""
   print("_check_config (rda_python_dsupdt {})".format(__version__))
   app = DsUpdt()
   dsids = [a for a in sys.argv[1:] if not a.startswith('-')]
   if dsids:
      cnd = "dsid IN ({})".format(', '.join("'{}'".format(d) for d in dsids))
   else:
      cnd = None
   counts = {}
   check_controls(app, cnd, counts)
   check_locfiles(app, cnd, counts)
   check_rmtfiles(app, cnd, counts)
   total = sum(counts.values())
   if total:
      print("\n{} invalid record(s): {}".format(total,
            ', '.join("{} {}".format(counts[t], t) for t in ('dcupdt', 'dlupdt', 'drupdt') if t in counts)))
   else:
      print("No invalid dsupdt configuration record found")
   sys.exit(1 if total else 0)


if __name__ == "__main__": main()
