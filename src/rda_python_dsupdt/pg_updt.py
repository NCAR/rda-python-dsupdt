#
###############################################################################
#
#     Title: pg_updt.py
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 09/23/2020
#             2025-02-07 transferred to package rda_python_dsupdt from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-08 transfer to class PgUpdt
#   Purpose: python library module to help rountinely updates of new data 
#             for one or multiple datasets
#
#    Github: https://github.com/NCAR/rda-python-dsupdt.git
#
###############################################################################
#
import os
import re
import time
from os import path as op
from rda_python_common.pg_cmd import PgCMD
from rda_python_common.pg_opt import PgOPT

class PgUpdt(PgOPT, PgCMD):
   """Base class providing common update utility functions for dataset updates.

   Extends PgOPT and PgCMD to handle update control records, local/remote file
   management, temporal pattern replacement, and dataset update scheduling in RDADB.
   Defines option codes, aliases, table hash mappings, and shared counters used by
   the DsUpdt subclass.
   """

   def __init__(self):
      """Initialize option/alias tables, table hash mappings, and update counters."""
      super().__init__()  # initialize parent class
      self.CORDERS = {}
      self.OPTS.update({
         'DR': [0x00010, 'DownloadRemote',2],
         'BL': [0x00020, 'BuildLocal',    2],
         'PB': [0x00030, 'ProcessBoth',   2], # DR & BL
         'AF': [0x00040, 'ArchiveFile',   2],
         'CF': [0x00080, 'CleanFile',     2],
         'UF': [0x000F0, 'UpdateFile',    2], # DR & BL & AF & CF
         'CU': [0x00200, 'CheckUpdate',   0],
         'GC': [0x00400, 'GetControl',    0],
         'GL': [0x00800, 'GetLocalFile',  0],
         'GR': [0x01000, 'GetRemoteFile', 0],
         'GA': [0x01C00, 'GetALL',        0], # GC & GL & GR
         'SC': [0x02000, 'SetControl',    1],
         'SL': [0x04000, 'SetLocalFile',  1],
         'SR': [0x08000, 'SetRemoteFile', 1],
         'SA': [0x0E000, 'SetALL',        4], # SC & SL & SR
         'DL': [0x20000, 'Delete',        1],
         'UL': [0x40000, 'UnLock',        1],
         'AW': [0, 'AnyWhere'],
         'BG': [0, 'BackGround'],
         'CA': [0, 'CheckAll'],
         'CN': [0, 'CheckNewer'],
         'CP': [0, 'CurrentPeriod'],
         'EE': [0, 'ErrorEmail'],   # send email when error happens only
         'FO': [0, 'FormatOutput'],
         'FU': [0, 'FutureUpdate'],
         'GZ': [0, 'GMTZone'],
         'HU': [0, 'HourlyUpdate'],
         'IE': [0, 'IgnoreError'],
         'KR': [0, 'KeepRemote'],
         'KS': [0, 'KeepServer'],
         'LO': [0, 'LogOn'],
         'MD': [0, 'MyDataset'],
         'MO': [0, 'MissedOnly'],
         'MU': [0, 'MultipleUpdate'],
         'NC': [0, 'NewControl'],
         'NE': [0, 'NoEmail'],
         'NL': [0, 'NewLocfile'],
         'NY': [0, 'NoLeapYear'],
         'QE': [0, 'QuitError'],
         'RA': [0, 'RetryArchive'],
         'RD': [0, 'RetryDownload'],
         'RE': [0, 'ResetEndTime'],
         'RO': [0, 'ResetOrder'],
         'SE': [0, 'SummaryEmail'],   # send summary email only
         'UB': [0, 'UseBeginTime'],
         'UT': [0, 'UpdateTime'],
         'AO': [1, 'ActOption',     1],  # default to <!>
         'CD': [1, 'CurrentDate', 256],  # used this instead of curdate()
         'CH': [1, 'CurrentHour',  16],  # used this instead of (localtime)[2]
         'DS': [1, 'Dataset',       0],
         'DV': [1, 'Divider',       1],  # default to <:>
         'ES': [1, 'EqualSign',     1],  # default to <=>
         'FN': [1, 'FieldNames',    0],
         'LN': [1, 'LoginName',     1],
         'OF': [1, 'OutputFile',    0],
         'ON': [1, 'OrderNames',    0],
         'PL': [1, 'ProcessLimit', 17],
         'VS': [1, 'ValidSize',    17],  # default to self.PGLOG['MINSIZE']
         'AN': [2, 'ActionName',      1],
         'AT': [2, 'AgeTime',         1],
         'BC': [2, 'BuildCommand',    1],
         'BP': [2, 'BatchProcess',    0, ''],
         'BT': [2, 'BeginTime',       1],
         'CC': [2, 'CarbonCopy',      0],
         'CI': [2, 'ControlIndex',   16],
         'CL': [2, 'CleanCommand',    1],
         'CO': [2, "ControlOffset",   1],
         'CT': [2, 'ControlTime',     32+356],
         'DB': [2, 'Debug',           0],
         'DC': [2, 'DownloadCommand', 1],
         'DE': [2, 'Description',    64],
         'DO': [2, 'DownloadOrder',  16],
         'DT': [2, 'DataTime',       1+32+256],
         'EC': [2, 'ErrorControl',    1, "NIQ"],
         'ED': [2, 'EndDate',       257],
         'EH': [2, 'EndHour',        33],
         'EP': [2, 'EndPeriod',       1],
         'ET': [2, 'EndTime',        33],
         'FA': [2, 'FileArchived',    0],
         'FQ': [2, 'Frequency',       1],
         'GP': [2, 'GenericPattern',  0],
         'HN': [2, "HostName",        1],
         'HO': [2, 'HourOffset',     17],
         'ID': [2, 'ControlID',       0],
         'IF': [2, 'InputFile',       0],
         'KF': [2, 'KeepFile',        1, "NRSB"],
         'LF': [2, 'LocalFile',       0],
         'LI': [2, 'LocalIndex',     17],
         'MC': [2, 'EMailControl',    1, "ASNEB"],
         'MR': [2, 'MissRemote',    128, "NY"],
         'DI': [2, 'DueInterval',     1],
         'OP': [2, 'Options',         1],
         'PD': [2, 'PatternDelimiter', 2], # pattern delimiters, default to ["<", ">"]
         'PI': [2, 'ParentIndex',    17],
         'PR': [2, 'ProcessRemote',   1],
         'QS': [2, 'QSubOptions',     0],
         'RF': [2, 'RemoteFile',      0],
         'RI': [2, 'RetryInterval',   1],
         'SF': [2, 'ServerFile',      0],
         'SN': [2, 'Specialist',      1],
         'TI': [2, 'TimeInterval',    1],
         'UC': [2, 'UpdateControl',   1],
         'VI': [2, 'ValidInterval',   1],
         'WD': [2, 'WorkDir',         1],
         'XC': [2, 'ExecuteCommand',  1],
         'XO': [2, 'ExecOrder',      16],
      })
      self.ALIAS.update({
         'AN': ['Action', "AC"],
         'AT': ['FileAge', "FileAgeTime"],
         'BC': ['BuildCmd'],
         'BG': ['b'],
         'BL': ['BuildLocalfile'],
         'BP': ['d', 'DelayedMode'],
         'BT': ['IT', 'InitialTime'],
         'CI': ['UpdateControlIndex'],
         'CL': ['CleanFile'],
         'CN': ['CheckNewFile'],
         'DC': ['Command', 'Download'],
         'DE': ['Desc', 'Note', 'FileDesc', 'FileDescription'],
         'DI': ['NextDue'],
         'DL': ['RM', 'Remove'],
         'DR': ['DownloadRemoteFile'],
         'DS': ['Dsid', 'DatasetID'],
         'DV': ['Delimiter', 'Separator'],
         'ED': ['UpdateEndDate'],
         'EH': ['UpdateEndHour'],
         'EP': ['EndPeriodDay'],
         'FA': ['SF', 'WF', 'QF'],
         'FQ': ['UpdateFrequency'],
         'FU': ["ForceUpdate"],
         'GC': ['GetUpdateControl'],
         'GL': ['GetLocal'],
         'GN': ['GroupID'],
         'GP': ['GeneralPattern'],
         'GR': ['GetRemote'],
         'GZ': ['GMT', 'GreenwichZone', 'UTC'],
         'HN': ['HostMachine'],
         'KR': ['KeepRemoteFile'],
         'KS': ['KeepServerFile'],
         'LF': ['LocalFileIndex'],
         'LI': ['LocIndex', "UpdateIndex"],
         'LO': ['LoggingOn'],
         'OP': ['DsarchOption'],
         'NC': ['NewUpdateControl'],
         'NL': ['NewLocalFile'],
         'PD': ['TD', 'TemporalDelimiter'],
         'QE': ['QuitOnError'],
         'QS': ['PBSOptions'],
         'RD': ['Redownload'],
         'RO': ['Reorder'],
         'SC': ['SetUpdateControl'],
         'SL': ['SetLocal'],
         'SN': ['SpecialistName'],
         'SR': ['SetRemote'],
         'TI': ['Interval'],
         'UL': ["UnLockUpdate"],
         'XC': ['ExecCmd'],
         'XO': ['ExecuteOrder']
      })
      # single letter short names for option 'FN' (Field Names) to retrieve info
      # from RDADB; only the fields can be manipulated by this application are listed
      #  SHORTNM KEYS(self.OPTS) DBFIELD
      self.TBLHASH['dlupdt'] = {      # condition flag, 0-int, 1-string, -1-exclude
         'L': ['LI', "lindex",        0],
         'F': ['LF', "locfile",       1],
         'A': ['AN', "action",        1],     # dsarch action
         'I': ['CI', "cindex",        0],
         'U': ['FA', "archfile",      1],
         'X': ['XO', "execorder",     1],
         'S': ['SN', "specialist",    1],
         'M': ['MR', "missremote",    1],
         'W': ['WD', "workdir",       1],
         'O': ['OP', "options",       1],
         'C': ['DC', "download",      1],
         'Q': ['FQ', "frequency",     1],
         'E': ['EP', "endperiod",     0],
         'J': ['ED', "enddate",       1],
         'K': ['EH', "endhour",       0],
         'N': ['DI', "nextdue",       1],
         'V': ['VI', "validint",      1],
         'T': ['AT', "agetime",       1],
         'R': ['PR', "processremote", 1],
         'B': ['BC', "buildcmd",      1],
         'Z': ['CL', "cleancmd",      1],
         'D': ['DE', "note",          1],
      }
      self.TBLHASH['drupdt'] = {
         'L': ['LI', "lindex",      0],   # same as dlupdt.lindex
         'F': ['RF', "remotefile",  1],
         'D': ['DO', "dindex",      0],
         'S': ['SF', "serverfile",  1],
         'C': ['DC', "download",    1],
         'B': ['BT', "begintime",   1],
         'E': ['ET', "endtime",     1],
         'T': ['TI', "tinterval",   1],
      }
      self.TBLHASH['dcupdt'] = {
         'C': ['CI', "cindex",     0],
         'L': ['ID', "cntlid",     1],
         'N': ['SN', "specialist", 1],
         'P': ['PI', "pindex",     0],   # if not 0, refer to another dcupdt.cindex
         'A': ['AN', "action",     1],   # dsupdt action
         'F': ['FQ', "frequency",  1],
         'O': ['CO', "cntloffset", 1],
         'T': ['CT', "cntltime",   1],
         'R': ['RI', "retryint",   1],
         'V': ['VI', "validint",   1],
         'U': ['UC', "updtcntl",   1],
         'J': ['MC', "emailcntl",  1],
         'E': ['EC', "errorcntl",  1],
         'K': ['KF', "keepfile",   1],
         'Z': ['HO', "houroffset", 1],
         'D': ['DT', "datatime",   1],
         'H': ['HN', "hostname",   1],
         'Q': ['QS', "qoptions",   1],
         'Y': ['CC', "emails",     1],
         'X': ['XC', "execcmd",    1],
      }
      # global info to be used by the whole application
      self.PGOPT['updated'] = 0
      self.PGOPT['AUTODS'] = 0
      self.PGOPT['CNTLACTS'] = self.OPTS['UF'][0]|self.OPTS['CU'][0]
      self.PGOPT['UPDTACTS'] = "AF|BL|CF|CU|DR|PB|UF"
      self.PGOPT['ARCHACTS'] = "AW|AS|AQ"
      self.PGOPT['DTIMES'] = {}
      self.PGOPT['UCNTL'] = {}
      #default fields for getting info
      self.PGOPT['dlupdt'] = "LFAXIUCOQJNVWRZ"
      self.PGOPT['drupdt'] = "LFDSCBET"
      self.PGOPT['dcupdt'] = "CLNPAFOTRVUJEKZ"
      #all fields for getting info
      self.PGOPT['dlall'] = "LFAXIUCOQEJKNVTWMRBZSD"
      self.PGOPT['drall'] = self.PGOPT['drupdt']
      self.PGOPT['dcall'] = "CLNPAFOTRVUJEKZDHSQYX"
      # remote file download status
      # 0 error download, but continue for further download
      # 1 successful full/partial download, continue for build local files
      # < 0 error download, stop
      self.PGOPT['rstat'] = 1   # default to successful download
      # counts
      self.PGOPT['PCNT'] = 1
      self.PGOPT['vcnt'] = self.PGOPT['rcnt'] = self.PGOPT['dcnt'] = self.PGOPT['lcnt'] = 0
      self.PGOPT['bcnt'] = self.PGOPT['acnt'] = self.PGOPT['mcnt'] = 0
      self.PGOPT['ucnt'] = self.PGOPT['upcnt'] = self.PGOPT['ubcnt'] = self.PGOPT['uhcnt'] = 0
      self.PGOPT['uscnt'] = self.PGOPT['qbcnt'] = self.PGOPT['qdcnt'] = 0
      self.PGOPT['uwcnt'] = self.PGOPT['udcnt'] = self.PGOPT['uncnt'] = self.PGOPT['rdcnt'] = 0
      self.PGOPT['lindex'] = 0   # the current lindex is under updating
      self.WSLOWS = {
         'nomads.ncep.noaa.gov': 8
      }
      # set default parameters
      self.params['PD'] = ["<" , ">"]   # temporal pattern delimiters
      self.params['PL'] = 1   # max number of child processes allowed

   # get file conition
   def file_condition(self, tname, include = None, exclude = None, nodsid = 0):
      """Build a SQL WHERE condition string for filtering update records.

      Iterates over the field hash for the given table name and constructs a
      condition from option values present in self.params.  A ``dsid`` prefix
      is prepended unless *nodsid* is set.

      Args:
         tname (str): Table name key into self.TBLHASH (e.g. 'dlupdt', 'drupdt').
         include (str | None): If given, only keys in this string are included.
         exclude (str | None): If given, keys in this string are skipped.
         nodsid (int): When non-zero, omit the leading ``dsid = '...'`` clause.

      Returns:
         str: SQL condition fragment (without a leading ``AND``).
      """
      condition = ""
      hash = self.TBLHASH[tname]
      noand = 1 if nodsid else 0
      if not hash: self.pglog(tname + ": not defined in self.TBLHASH", self.PGOPT['extlog'])
      for key in hash:
         if include and include.find(key) < 0: continue
         if exclude and exclude.find(key) > -1: continue
         type = hash[key][2]
         if type < 0: continue   # exclude
         opt = hash[key][0]
         if opt not in self.params: continue
         fld = hash[key][1]
         condition += self.get_field_condition(fld, self.params[opt], type, noand)
         noand = 0
      if not nodsid:
         condition =  "dsid = '{}'{}".format(self.params['DS'], condition)
      return condition

   # check if enough information entered on command line and/or input file
   # for given action(s)
   def check_enough_options(self, cact, acts):
      """Validate that sufficient options have been supplied for the requested action.

      Raises an action error if required indices or file names are missing.
      Also performs one-time setup tasks: sets the uid, configures batch/daemon
      mode, applies no-leap-year and email suppression flags, and starts the
      none-daemon process manager.

      Args:
         cact (str): The current action key (e.g. 'UF', 'SC', 'SL').
         acts (int): Bitmask of all requested actions from self.PGOPT['ACTS'].
      """
      errmsg = [
         "Miss dataset number per -DS(-Dataset)",
         "Miss local file names per -LF(-LocalFile)",
         "Miss remote file names per -RF(-RemoteFile)",
         "Miss local Index per -LI(-LocalIndex)",
         "Miss Control Index per -CI(-ControlIndex)",
         "Process one Update Control Index at a time",
      ]
      erridx = -1
      lcnt = ccnt = 0
      if 'LI' in self.params: lcnt = self.validate_lindices(cact)
      if 'CI' in self.params or 'ID' in self.params: ccnt = self.validate_cindices(cact)
      if self.OPTS[cact][2] == 1:
         if acts&self.OPTS['SC'][0]:
            if 'CI' not in self.params: erridx = 4
         elif cact == 'DL' or cact == 'UL':
            if not ('LI' in self.params or 'CI' in self.params): erridx = 3
         elif 'LI' not in self.params:
            erridx = 3
         elif acts&self.OPTS['SR'][0] and 'RF' not in self.params:
            erridx = 2
         if erridx < 0:
            if (lcnt + ccnt) > 0:
               if 'DS' not in self.params:
                  erridx = 0
               elif lcnt > 0 and cact == 'SL' and 'LF' not in self.params:
                  erridx = 1
      elif self.OPTS[cact][2] == 2:
         if 'CI' in self.params and len(self.params['CI']) > 1:
            erridx = 5
      if erridx >= 0: self.action_error(errmsg[erridx], cact)
      self.set_uid("dsupdt")   # set uid before any action
      if 'VS' in self.params:   # minimal size for a file to be valid for archive
         self.PGLOG['MINSIZE'] = int(self.params['VS'])
      if 'BP' in self.params:
         if 'PL' in self.params: self.params['PL'] = 1
         if 'CI' in self.params:
            oidx = self.params['CI'][0]
            otype = 'C'
         elif 'LI' in self.params:
            oidx = self.params['LI'][0]
            otype = 'L'
         else:
            oidx = 0
            otype = ''
         # set command line Batch options
         self.set_batch_options(self.params, 2, 1)
         self.init_dscheck(oidx, otype, "dsupdt", self.get_dsupdt_dataset(),
                            cact, "" if 'AW' in self.params else self.PGLOG['CURDIR'], self.params['LN'],
                            self.params['BP'], self.PGOPT['extlog'])
      if 'NY' in self.params: self.PGLOG['NOLEAP'] = 1
      if 'NE' in self.params:
         self.PGLOG['LOGMASK'] &= ~self.EMLALL   # turn off all email acts
      else:
         if 'SE' in self.params: self.PGOPT['emllog'] |= self.EMEROL
         if 'CC' in self.params and (self.PGOPT['ACTS']&self.OPTS['SC'][2]) == 2: self.add_carbon_copy(self.params['CC'])
      if self.PGOPT['ACTS']&self.OPTS['UF'][0]:
         plimit = self.params['PL'] if 'PL' in self.params else 1
         logon = self.params['LO'] if 'LO' in self.params else 1
         self.start_none_daemon('dsupdt', self.PGOPT['CACT'], self.params['LN'], plimit, 120, logon)
      else:
         self.start_none_daemon('dsupdt', self.PGOPT['CACT'], self.params['LN'], 1, 120, 1)
      if self.PGSIG['MPROC'] > 1:
         self.PGOPT['emllog'] |= self.FRCLOG
         self.PGOPT['wrnlog'] |= self.FRCLOG

   # get the associated dataset id
   def get_dsupdt_dataset(self):
      """Return the dataset ID associated with the current operation.

      Checks self.params in order: 'DS', 'CI' (control index), 'LI' (local index).

      Returns:
         str | None: Dataset ID string, or None if not determinable.
      """
      if 'DS' in self.params: return self.params['DS']
      if 'CI' in self.params and self.params['CI'][0]:
         pgrec = self.pgget("dcupdt", "dsid", "cindex = {}".format(self.params['CI'][0]), self.PGOPT['extlog'])
         if pgrec: return pgrec['dsid']
      if 'LI' in self.params and self.params['LI'][0]:
         pgrec = self.pgget("dlupdt", "dsid", "lindex = {}".format(self.params['LI'][0]), self.PGOPT['extlog'])
         if pgrec: return pgrec['dsid']
      return None

   # replace the temporal patterns in given fname with date/hour
   # return pattern array only if not date
   def replace_pattern(self, fname, date, hour = None, intv = None, limit = 0, bdate = None, bhour = None):
      """Replace temporal placeholder patterns in a filename with actual date/hour values.

      Patterns are delimited by self.params['PD'] (default ``<`` and ``>``).
      Supported pattern types include: generic (``P<n>``), current-date (``C…C``),
      month-fraction (``M[NC]M``), sequence number (``N[HD]+N``), back-shifted
      (``B…B``), Wednesday-aligned (``W…W``), and standard date-format strings.

      Args:
         fname (str | None): Filename containing zero or more patterns.
         date (str | int | None): End data date (YYYY-MM-DD).
         hour (int | None): End data hour (0-23), or None for daily data.
         intv (list | None): Frequency interval array [Y, M, D, H, …].
         limit (int): Maximum number of patterns to replace (0 = unlimited).
         bdate (str | None): Beginning date of the update period.
         bhour (int | None): Beginning hour of the update period.

      Returns:
         str | None: Filename with patterns replaced, or None if *fname* is falsy.
      """
      if not fname: return None
      if date and not isinstance(date, str): date = str(date)
      if bdate and not isinstance(bdate, str): bdate = str(bdate)
      seps = self.params['PD']
      match = r"[^{}]+".format(seps[1])
      patterns = re.findall(r'{}([^{}]+){}'.format(seps[0], seps[1], seps[1]), fname)
      pcnt = len(patterns)
      if pcnt == 0: return fname   # return original name if no pattern
      if limit and pcnt > limit: pcnt = limit
      mps = {'b': r'^B(.+)B$', 'c': r'^C(.+)C$', 'd': r'(\d+)$', 'm': r'^M([NC])M$',
             'n': r'^N(H+|D+)N$', 'p': r'^P(\d+)$', 's': r'^S[\d:]+S$', 'w': r'^W(.+)W$'}
      for i in range(pcnt):
         pattern = patterns[i]
         replace = "{}{}{}".format(seps[0], pattern, seps[1])
         d = None
         domatch = 1
         ms = re.match(mps['p'], pattern, re.I)
         if ms:    # generic pattern matches
            pidx = int(ms.group(1))
            pattern = self.params['GP'][pidx] if 'GP' in self.params else None
            if not pattern: self.pglog("{}: MISS value per option -GP for matching general pattern '{}'".format(fname, replace), self.PGOPT['extlog'])
            domatch = 1
         if domatch:
            ms = re.match(mps['c'], pattern, re.I)  # current date
            if ms:
               pattern = ms.group(1)
               d = self.params['CD']
               h = self.params['CH']
               domatch = 0
         if domatch and (not date or re.match(mps['s'], pattern, re.I)): continue
         if domatch:
            ms = re.match(mps['m'], pattern, re.I)
            if ms:
               pattern = ms.group(1)
               if intv and len(intv) == 7 and intv[6] and re.search(mps['d'], date):
                  ms = re.search(mps['d'], date)
                  d = int(ms.group(1))
                  d = (intv[6] - 1) if d >= 28 else int(d * intv[6] / 30)
                  if pattern == "C":
                     pattern = chr(65 + d)   # upper case, chr(65) is A
                  elif pattern == "c":
                     pattern = chr(97 + d)   # lower case, chr(97) is a
                  else:
                     pattern = str(d + 1)   # numeric, start from 1
                  d = None
                  domatch = 0
               else:
                  self.pglog("{}: MISS month fraction for '{}'".format(fname, replace), self.PGOPT['emllog'])
         if domatch:
            ms = re.match(mps['n'], pattern, re.I)
            if ms:
               pattern = ms.group(1)
               if not bdate: (bdate, bhour) = self.addfrequency(date, hour, intv, 0)
               plen = len(pattern)
               if re.match(r'^D', pattern):
                  diff = self.diffdate(date, bdate)
               else:
                  diff = self.diffdatehour(date, hour, bdate, bhour)
               pattern = "{:0{}}".format(diff, plen)
               domatch = 0
         if domatch:
            ms = re.match(mps['b'], pattern, re.I)
            if ms:
               pattern = ms.group(1)
               d = date
            elif 'UB' in self.params:
               d = date
            if d and intv:    # beginning time of update period
               if bdate:
                  d = bdate
                  h = bhour
               else:
                  (d, h) = self.addfrequency(d, hour, intv, 0)
            else:
               ms = re.match(mps['w'], pattern, re.I)
               if ms:   # back to the nearest Wed
                  pattern = ms.group(1)
                  wd = self.get_weekday(date)
                  if wd < 3:
                     wd += 4
                  else:
                     wd -= 3
                  d = self.adddate(date, 0, 0, -wd) if (wd > 0) else date
               else:
                  d = date
               h = hour
         if d: pattern = self.format_datehour(d, h, pattern)
         fname = re.sub(replace, pattern, fname, 1)
      return fname

   # get next display order of an archived data file of given dataset (and group)
   def get_next_exec_order(self, dsid, next):
      """Return the next execution order value for a local file record of *dsid*.

      Caches the current maximum ``execorder`` per dataset in self.CORDERS.
      Passing *dsid* as None resets the cache.

      Args:
         dsid (str | None): Dataset ID, or None to reset the cache.
         next (int): If non-zero, query the database for the current maximum
            order when the dataset is not already cached.

      Returns:
         int | None: Incremented execution order, or None when *dsid* is None.
      """
      if not dsid:
         self.CORDERS = {}   # reinitialize cached display orders
         return
      if dsid not in self.CORDERS:
         if next:
            pgrec = self.pgget("dlupdt", "max(execorder) max_order", "dsid = '{}'".format(dsid), self.PGOPT['extlog'])
            self.CORDERS[dsid] = pgrec['max_order'] if pgrec else 0
      self.CORDERS[dsid] += 1
      return self.CORDERS[dsid]

   # execute specialist specified command
   def executable_command(self, cmd, file, dsid, edate, ehour, rfiles = None):
      """Expand placeholder tokens in a specialist-defined shell command string.

      Replaces ``__FN__``/``__FNAME__``/``__FILENAME__``, ``-LF``/``-RF``/``-SF``,
      ``-DS``, ``-ED``, ``-EH``, ``-SN``/``-LN``, and ``-LI`` with the
      corresponding runtime values.  Returns None for commented-out (``#``) or
      empty commands.

      Args:
         cmd (str | None): Raw command template.
         file (str | None): Local or server filename to substitute.
         dsid (str | None): Dataset ID.
         edate (str | int | None): End data date.
         ehour (int | None): End data hour.
         rfiles (list | None): List of remote file names or dicts for ``-RF``.

      Returns:
         str | None: Expanded command string, or None if *cmd* is falsy/commented.
      """
      if not cmd or re.match(r'^#', cmd): return None
      if re.search(r'\$', cmd): cmd = self.replace_environments(cmd, None, self.PGOPT['emlerr'])
      if file:
         ms = re.search(r'__(FN|FNAME|FILENAME)__', cmd)
         if ms:
            cmd = re.sub(r'__{}__'.format(ms.group(1)), file, cmd)
         elif re.search(r'(-LF|-RF|-SF)', cmd):
            ms = re.search(r'(-LF|-RF|-SF)', cmd)
            cmd = re.sub(ms.group(1), file, cmd)
         elif re.search(r'/$', cmd):
            cmd += file
            if re.search(r'(^|\s|\||\S/)msrcp\s', cmd):
               cmd += " file"
            elif re.search(r'(^|\s|\||\S/)(cp|mv)\s', cmd):
               cmd += " ."
         elif cmd.find(file) < 0 and re.search(r'(^|\s|\||\S/)(rm\s|tar\s.+\.tar$)', cmd):
            cmd += " file"
      if re.search(r'-RF', cmd):
         names = []
         if rfiles:
            for rfile in rfiles:
               if isinstance(rfile, dict):
                  names.append(rfile['fname'])
               else:
                  names.append(rfile)
         name = ' '.join(names)
         cmd = re.sub(r'-RF', name, cmd, 1)
      if re.search(r'-DS', cmd):
         name = dsid if dsid else ""
         cmd = re.sub(r'-DS', name, cmd, 1)
      if edate and re.search(r'-ED', cmd):
         name = str(edate) if edate else ""
         cmd = re.sub('-ED', name, cmd, 1)
      if re.search(r'-EH', cmd):
         name = str(ehour) if ehour != None else ''
         cmd = re.sub(r'-EH', name, cmd, 1)
      ms = re.search(r'(-SN|-LN)', cmd)
      if ms:
         cmd = re.sub(ms.group(1), self.params['LN'], cmd, 1)
      if re.search(r'-LI', cmd):
         name = str(self.PGOPT['lindex']) if self.PGOPT['lindex'] else ''
         cmd = re.sub(r'-LI', name, cmd, 1)
      return cmd

   # get the local file names
   def get_local_names(self, lfile, tempinfo, edate = None):
      """Resolve the list of local filenames for a given update period.

      If *lfile* starts with ``!``, the remainder is treated as an executable
      command whose stdout (``::``-delimited) provides the names.  Otherwise
      serial patterns are expanded and temporal placeholders replaced.

      Args:
         lfile (str): Local file pattern or ``!command``.
         tempinfo (dict): Temporal info dict (must contain 'ehour', 'edate').
         edate (str | None): Override end date; defaults to tempinfo['edate'].

      Returns:
         list | None: List of resolved filenames, or None if none found.
      """
      locfiles = []
      ehour = tempinfo['ehour']
      if not edate: edate = tempinfo['edate']
      if lfile[0] == '!':   # executable for build up local file names
         cmd = self.executable_command(lfile[1:], None, self.params['DS'], edate, ehour)
         if not cmd: return 0
         buf = self.pgsystem(cmd, self.PGOPT['wrnlog'], 21)
         if not buf: return self.pglog(lfile + ": NO local filename returned", self.PGOPT['emlerr'])
         locfiles = re.split('::', buf)
      else:
         lfiles = self.expand_serial_pattern(lfile)
         lcnt = len(lfiles)
         for i in range(lcnt):
            locfiles.append(self.replace_pattern(lfiles[i], edate, ehour, tempinfo['FQ']))
      return locfiles if locfiles else None

   # expend serial pattern
   def expand_serial_pattern(self, fname):
      """Expand a serial index pattern (``<S start:end:step S>``) in a filename.

      The pattern ``<Sstart:end:stepS>`` (or ``<Sstart:endS>`` with step=1) is
      replaced by each zero-padded integer in the range, producing a list of
      filenames.  Returns a single-element list with the original name when no
      such pattern is found.

      Args:
         fname (str | None): Filename possibly containing a serial pattern.

      Returns:
         list | None: Expanded list of filenames, or None if *fname* is falsy.
      """
      if not fname: return None
      seps = self.params['PD']
      ms = re.search(r'{}S(\d[\d:]+\d)S{}'.format(seps[0], seps[1]), fname)
      if not ms: return [fname]
      rep = "{}S{}S{}".format(seps[0], ms.group(1), seps[1])
      mcs = re.split(':', ms.group(1))
      tlen = len(mcs[0])
      idx = [0]*3
      idx[0] = int(mcs[0])
      idx[1] = int(mcs[1])
      idx[2] = int(mcs[2]) if len(mcs) > 2 else 1
      fns = []
      i = idx[0]
      while i <= idx[1]:
         val = "{:0{}}".format(i, tlen)
         fn = re.sub(rep, val, fname, 1)
         fns.append(fn)
         i += idx[2]
      return fns

   # get the remote file names
   def get_remote_names(self, rfile, rmtrec, rmtinfo, tempinfo, edate = None):
      """Resolve the list of remote filenames for a given update period.

      If *rfile* starts with ``!``, the remainder is executed as a command.
      Otherwise serial patterns are expanded and each element is passed through
      replace_remote_pattern_times() to produce one or more timestamped names.

      Args:
         rfile (str): Remote file pattern or ``!command``.
         rmtrec (dict): Remote file database record (tinterval, begintime, etc.).
         rmtinfo (str): Log-friendly label for this remote entry.
         tempinfo (dict): Temporal info dict for the current update period.
         edate (str | None): Override end date; defaults to tempinfo['edate'].

      Returns:
         list | None: List of remote file dicts (with 'fname', 'ready', etc.),
         or None if none found.
      """
      rmtfiles = []
      if not edate: edate = tempinfo['edate']
      if rfile[0] == '!':   # executable for build up remote file names
         cmd = self.executable_command(rfile[1:], None, self.params['DS'], edate, tempinfo['ehour'])
         if not cmd: return None
         rfile = self.pgsystem(cmd, self.PGOPT['wrnlog'], 21)
         if not rfile: return self.pglog(rmtinfo + ": NO remote filename returned", self.PGOPT['emlerr'])
         rmtfiles = re.split('::', rfile)
      else:
         rfiles = self.expand_serial_pattern(rfile)
         rcnt = len(rfiles)
         for i in range(rcnt):
            rmtfiles.extend(self.replace_remote_pattern_times(rfiles[i], rmtrec, rmtinfo, tempinfo, edate))
      return rmtfiles if rmtfiles else None

   # get and replace pattern dates/hours for remote files
   def replace_remote_pattern_times(self, rfile, rmtrec, rmtinfo, tempinfo, edate = None):
      """Expand a single remote filename pattern over its time interval range.

      When a remote record has a ``tinterval`` field, this method generates one
      remote file dict per sub-period within the update window (from begintime
      to endtime).  Without a time interval a single dict is returned.

      Args:
         rfile (str): Remote file pattern with optional temporal placeholders.
         rmtrec (dict): Remote file DB record (tinterval, begintime, endtime).
         rmtinfo (str): Log label for this remote record.
         tempinfo (dict): Temporal info for the current update period.
         edate (str | None): Override end date; defaults to tempinfo['edate'].

      Returns:
         list: List of remote file dicts; empty on error.
      """
      rfiles = []
      if not edate: edate = tempinfo['edate']
      ehour = tempinfo['ehour']
      freq = tempinfo['FQ']
      (bdate, bhour) = self.addfrequency(edate, ehour, freq, 0)
      funit = tempinfo['QU'] if tempinfo['QU'] else None
      tintv = rmtrec['tinterval'] if rmtrec['tinterval'] else None
      if not tintv:
         if rmtrec['dindex'] and funit:
            if self.need_time_interval(rfile, freq): return []
         rfiles = [self.one_remote_filename(rfile, edate, ehour, tempinfo, None, bdate, bhour)]
         return rfiles
      elif not funit:
         self.pglog("{}: MISS Update Frequency for given time interval '{}'".format(rmtinfo, tintv), self.PGOPT['emlerr'])
         return []
      ms = re.match(r'^(\d*)([YMWDH])$', tintv)
      if ms:
         val = int(ms.group(1)) if len(ms.group(1)) > 0  else 1
         unit = ms.group(2)
         if unit == 'W': val *= 7
      else:
         self.pglog("{}: time interval '{}' NOT in (Y,M,W,D,H)".format(rmtinfo, tintv), self.PGOPT['emlerr'])
         return []
      # check if multiple data periods
      i = 0   # not single period
      if unit == 'H':
         if freq[3] and freq[3] <= val: i = 1
      elif unit == 'D' or unit == 'W':
         if freq[3] or freq[2] and freq[2] <= val: i = 1
      elif unit == 'M':
         if freq[3] or freq[2] or freq[1] and freq[1] <= val: i = 1
      elif unit == 'Y':
         if not freq[0] or freq[0] <= val: i = 1
      if i == 1:
         rfiles = [self.one_remote_filename(rfile, edate, ehour, tempinfo, None, bdate, bhour)]
         return rfiles
      date = edate
      hour = ehour
      # set ending date/hour for multiple data periods
      max = self.replace_pattern(rmtrec['endtime'], date, 0) if rmtrec['endtime'] else 0
      if max:
         ms = re.match(r'^(\d+-\d+-\d+)', max)
         if ms:
            edate = ms.group(1)
            ms = re.search(r':(\d+)', max)
            if ms: ehour = int(ms.group(1))
            max = 0
         else:
            if freq[1] and max.find(':') > -1:
               maxs = re.split(':', max)
               if len(maxs) == 12:
                  mn = 1
                  ms = re.match(r'^(\d+)-(\d+)', bdate)
                  if ms: mn = int(ms.group(2))
                  max = int(maxs[mn - 1])
               else: # use the first one
                  max = int(maxs[0])
            if max:
               if unit == 'H':
                  (edate, ehour) = self.adddatehour(bdate, bhour, 0, 0, 0, max)
               elif unit == 'Y':
                  edate = self.adddate(bdate, max, 0, 0)
               elif unit == 'M':
                  edate = self.adddate(bdate, 0, max, 0)
               elif unit == 'W' or unit == 'D':
                  edate = self.adddate(bdate, 0, 0, max)
      # set beginning date/hour for multiple data periods
      min = self.replace_pattern(rmtrec['begintime'], date, 0) if rmtrec['begintime'] else 0
      if min:
         ms = re.match(r'^(\d+-\d+-\d+)', min)
         if ms:
            date = ms.group(1)
            ms = re.search(r':(\d+)', min)
            if ms:
               hour = int(ms.group(1))
            else:
               hour = 0
            min = 0
         else:
            date = bdate
            hour = bhour
            if freq[1] and min.find(':') > -1:
               mins = re.split(':', min)
               if len(mins) == 12:
                  mn = 1
                  ms = re.match(r'^(\d+)-(\d+)', date)
                  if ms: mn = int(ms.group(2))
                  min = int(mins[mn-1])
               else: # use the first one
                  min = int(mins[0])
      else:
         date = bdate
         hour = bhour
   
      if min and not isinstance(min, int): min = int(min)
      gotintv = 0
      intv = [0]*4
      if unit == 'Y':
         intv[0] = val
         gotintv += 1
         if min: date = self.adddate(date, min, 0, 0)
      elif unit == 'M':
         intv[1] = val
         gotintv += 1
         if min:
            date = self.adddate(date, 0, min, 0)
         else:
            date = self.enddate(date, 0, 'M')
      elif unit == 'W' or unit == 'D':
         intv[2] = val
         gotintv += 1
         if min: date = self.adddate(date, 0, 0, min)
      elif unit == 'H':
         intv[3] = val
         gotintv += 1
         if hour is None or not freq[3]:
            ehour = 23
            hour = 0
         if min: (date, hour)  = self.adddatehour(date, hour, 0, 0, 0, min)
      if not gotintv:
         self.pglog("{}: error processing time interval '{}'".format(rmtinfo, tintv), self.PGOPT['emlerr'])
         return []
      rfiles = []
      i = 0
      while self.diffdatehour(date, hour, edate, ehour) <= 0:
         rfiles.append(self.one_remote_filename(rfile, date, hour, tempinfo, intv, bdate, bhour))
         (date, hour) = self.adddatehour(date, hour, intv[0], intv[1], intv[2], intv[3])
      return rfiles

   # get one hash array for a single remote file name
   def one_remote_filename(self, fname, date, hour, tempinfo, intv, bdate, bhour):
      """Build a remote file descriptor dict for a single date/hour.

      Resolves the filename pattern, determines readiness relative to the
      current date, and packages metadata (date, hour, time string, download
      command) into a dict used downstream by download_remote_files().

      Args:
         fname (str): Remote file pattern.
         date (str): Data date for this file (YYYY-MM-DD).
         hour (int | None): Data hour, or None for daily data.
         tempinfo (dict): Temporal info dict (FQ, NX, VD/VH, DC, etc.).
         intv (list | None): Interval array; falls back to tempinfo['FQ'].
         bdate (str): Beginning date of the update period.
         bhour (int | None): Beginning hour of the update period.

      Returns:
         dict: Remote file descriptor with keys 'fname', 'ready', 'amiss',
         'date', 'hour', 'time', and 'rcmd'.
      """
      if tempinfo['NX']:
         (udate, uhour) = self.adddatehour(date, hour, tempinfo['NX'][0], tempinfo['NX'][1], tempinfo['NX'][2], tempinfo['NX'][3])
      else:
         udate = date
         uhour = hour
      if 'CP' in self.params:
         (vdate, vhour) = self.addfrequency(self.PGOPT['CURDATE'], self.PGOPT['CURHOUR'], tempinfo['FQ'], 1)
      else:
         vdate = self.PGOPT['CURDATE']
         vhour = self.PGOPT['CURHOUR']
      rfile = {}
      if intv is None: intv = tempinfo['FQ']
      rfile['fname'] = self.replace_pattern(fname, date, hour, intv, 0, bdate, bhour)
      if 'FU' in self.params or self.diffdatehour(udate, uhour, vdate, vhour) <= 0:
         if tempinfo['VD'] and self.diffdatehour(date, hour, tempinfo['VD'], tempinfo['VH']) < 0:
            rfile['ready'] = -1
         else:
            rfile['ready'] = 1
      else:
         rfile['ready'] = 0
      rfile['amiss'] = 1 if (tempinfo['amiss'] == 'Y') else 0
      rfile['date'] = date
      rfile['hour'] = hour
      if hour is None:
         rfile['time'] = "23:59:59"
      else:
         rfile['time'] = "{:02}:00:00".format(hour)
      if tempinfo['DC']:
         rfile['rcmd'] = self.replace_pattern(tempinfo['DC'], date, hour, intv, 0, bdate, bhour)
      else:
         rfile['rcmd'] = None
      return rfile

   # record the date/hour for missing data
   def set_miss_time(self, lfile, locrec, tempinfo, rmonly = 0):
      """Update the missdate/misshour fields in the dlupdt record for a local file.

      When valid-interval tracking is active, this records the earliest date at
      which remote data became missing.  Passing *rmonly* = 1 clears the miss
      timestamp only when it matches the current end date/hour.

      Args:
         lfile (str): Local filename (used for logging context).
         locrec (dict): Local file DB record (lindex, missdate, misshour, etc.).
         tempinfo (dict): Temporal info dict (edate, ehour, VD, VH).
         rmonly (int): If 1, remove the miss-time rather than set it.

      Returns:
         int: 1 if a miss-time remains set, 0 otherwise.
      """
      setmiss = 1
      mdate = mhour = None
      pgrec = {}
      if rmonly:
         if(not locrec['missdate'] or 
            self.diffdatehour(tempinfo['edate'], tempinfo['ehour'], locrec['missdate'], locrec['misshour'])):
            return setmiss   # do not remove if miss times not match
      elif self.diffdatehour(tempinfo['edate'], tempinfo['ehour'], tempinfo['VD'], tempinfo['VH']) >= 0:
         mdate = tempinfo['edate']
         if tempinfo['ehour'] is not None: mhour = tempinfo['ehour']
         setmiss = 0
      if locrec['missdate']:
         if not mdate:
            pgrec['missdate'] = pgrec['misshour'] = None
         elif (self.diffdatehour(mdate, mhour, locrec['missdate'], locrec['misshour']) and
               self.diffdatehour(locrec['missdate'], locrec['misshour'], tempinfo['VD'], tempinfo['VH']) < 0):
            pgrec['missdate'] = mdate
            pgrec['misshour'] = mhour
      elif mdate:
         pgrec['missdate'] = mdate
         pgrec['misshour'] = mhour
      if not pgrec:
         if locrec['misshour']:
            if mhour is None or mhour != locrec['misshour']:
               pgrec['misshour'] = mhour
         elif mhour is not None:
            pgrec['misshour'] = mhour
      if pgrec: self.pgupdt("dlupdt", pgrec, "lindex = {}".format(locrec['lindex']), self.PGOPT['extlog'])
      return setmiss

   # reset next data end/update times
   def reset_update_time(self, locinfo, locrec, tempinfo, arccnt, endonly):
      """Advance the dlupdt end-date and optionally update the dsperiod record.

      After successfully archiving files, advances the stored ``enddate``/
      ``endhour`` to the next update period.  Also calls ``sdp`` to extend
      the dataset time coverage when the data period has grown.

      Args:
         locinfo (str): Log label for the current local file.
         locrec (dict): Local file DB record (lindex, enddate, endhour, gindex, etc.).
         tempinfo (dict): Temporal info (edate, ehour, FQ, EP, QU, NX).
         arccnt (int): Number of files successfully archived in this pass.
         endonly (int): If non-zero, skip the enddate advancement when no files
            were archived.
      """
      gx = 1 if re.search(r'(^|\s)-GX(\s|$)', locrec['options'], re.I) else 0
      date = tempinfo['edate']
      hour = tempinfo['ehour']
      if not gx and ('UT' in self.params or arccnt > 0):
         pgrec = self.get_period_record(locrec['gindex'], self.params['DS'], locinfo)
         if pgrec:
            ehour = None
            if hour != None:
                ms = re.match(r'^(\d+):', str(pgrec['time_end']))
                if ms: ehour = int(ms.group(1))
            diff = self.diffdatehour(date, hour, pgrec['date_end'], ehour)
            if 'UT' in self.params or diff > 0:
               sdpcmd = "sdp -d {} -g {} -ed {}".format(self.params['DS'][2:], pgrec['gindex'], date)
               if hour != None: sdpcmd += " -et {:02}:59:59".format(hour)
               if self.pgsystem(sdpcmd, self.MSGLOG, 32):
                  einfo = "{}".format(date)
                  if hour != None: einfo += ":{:02}".format(hour)
                  self.pglog("{}: data archive period {} to {}".format(locinfo, ("EXTENDED" if diff > 0 else "CHANGED"), einfo), self.PGOPT['emllog'])
      if not tempinfo['FQ'] or endonly and arccnt < 1: return
      if self.diffdatehour(date, hour, self.params['CD'], self.params['CH']) <= 0:
         (date, hour) = self.addfrequency(date, hour, tempinfo['FQ'], 1)
         date = self.enddate(date, tempinfo['EP'], tempinfo['QU'], tempinfo['FQ'][6])
      if 'UT' in self.params or not locrec['enddate'] or self.diffdatehour(date, hour, locrec['enddate'], locrec['endhour']) > 0:
         record = {'enddate': date}
         if hour != None:
            record['endhour'] = hour
            einfo = "end data date:hour {}:{:02}".format(date, hour)
         else:
            einfo = "end data date {}".format(date)
         if 'GZ' in self.params: einfo += "(UTC)"
         if tempinfo['NX']:
            (date, hour) = self.adddatehour(date, hour, tempinfo['NX'][0], tempinfo['NX'][1], tempinfo['NX'][2], tempinfo['NX'][3])
         if(locrec['enddate'] and
            self.pgupdt("dlupdt", record, "lindex = {}".format(locrec['lindex']), self.PGOPT['extlog'])):
            self.pglog("{}: {} {} for NEXT update".format(locinfo, ("set" if arccnt > 0 else "SKIP to"), einfo), self.PGOPT['emllog'])
            if self.PGOPT['UCNTL']: self.reset_data_time(tempinfo['QU'], tempinfo['edate'], tempinfo['ehour'], locrec['lindex'])
         else:
            self.pglog("{}: {} for NEXT update".format(locinfo, einfo), self.PGOPT['emllog'])
      else:
         if locrec['endhour'] != None:
            einfo = "end data date:hour {}:{:02}".format(locrec['enddate'], locrec['endhour'])
         else:
            einfo = "end data date {}".format(locrec['enddate'])
         if 'GZ' in self.params: einfo += "(UTC)"
         self.pglog("{}: ALREADY set {} for NEXT update".format(locinfo, einfo), self.PGOPT['emllog'])
         if self.PGOPT['UCNTL']: self.reset_data_time(tempinfo['QU'], tempinfo['edate'], tempinfo['ehour'], locrec['lindex'])

   # get period record for sub group
   def get_period_record(self, gindex, dsid, locinfo):
      """Fetch the dsperiod record for a dataset group, traversing parent groups.

      Looks up the period record for *gindex* in dsperiod; if not found and the
      group has a parent (dsgroup.pindex), recurses to find the parent's period.
      Logs an error and returns None if date_end is the sentinel '0000-00-00'.

      Args:
         gindex (int): Group index to look up.
         dsid (str): Dataset ID.
         locinfo (str): Log label for error messages.

      Returns:
         dict | None: dsperiod record dict, or None if not found/invalid.
      """
      pgrec = self.pgget("dsperiod", "gindex, date_end, time_end, dorder",
                          "dsid = '{}' AND gindex = {} ORDER BY dorder".format(dsid, gindex), self.PGOPT['extlog'])
      if not pgrec and gindex:
         pgrec = self.pgget("dsgroup", "pindex", "dsid = '{}' AND gindex = {}".format(dsid, gindex), self.PGOPT['extlog'])
         if pgrec: pgrec = self.get_period_record(pgrec['pindex'], dsid, locinfo)
      if pgrec and pgrec['date_end'] and pgrec['date_end'] == "0000-00-00":
         self.pglog(locinfo + ": dsperiod.date_end set as '0000-00-00' by 'gatherxml'", self.PGOPT['emlerr'])
         pgrec = None
      return pgrec

   # check if need time interval for remote/server file
   def need_time_interval(self, fname, freq):
      """Return True if the remote filename pattern implies a finer time interval.

      Warns when the temporal units embedded in *fname* are at a finer
      granularity than the dataset update frequency, suggesting that a
      ``tinterval`` should be set in the remote file record.

      Args:
         fname (str): Remote filename pattern.
         freq (list): Frequency array [Y, M, D, H, …] from get_control_time().

      Returns:
         int: 1 if a time interval appears needed, 0 otherwise.
      """
      units = self.temporal_pattern_units(fname, self.params['PD'])
      if not units: return 0   # no temporal pattern found in file name    
      funit = punit = None
      if freq[2] > 0:
         if 'H' in units:
            punit = "Hourly"
            funit = "Daily"
      elif freq[1] > 0:
         if 'H' in units:
            punit = "Hourly"
         elif 'D' in units:
            punit = "Daily"
         if punit: funit = "Monthly"
      elif freq[0] > 0:
         if 'H' in units:
            punit = "Hourly"
         elif 'D' in units:
            punit = "Daily"
         elif 'M' in units:
            punit = "Monthly"
         if punit: funit = "Yearly"
      if punit:
         self.pglog("{}: Remote File Name seems defined at {} Time Interval for {} Update, ".format(fname, punit, funit) +
                     "specify the Time Interval in remote file record to continue", self.PGOPT['emllog'])
         return 1
      else:
         return 0

   # check if local file is a growing one
   def is_growing_file(self, fname, freq):
      """Determine whether a local file is expected to grow with each update.

      A file is considered *growing* when its name contains no temporal pattern
      that matches the update frequency granularity, meaning the same physical
      file is appended to rather than replaced each cycle.

      Args:
         fname (str): Local filename pattern.
         freq (list): Frequency array [Y, M, D, H, …].

      Returns:
         int: 1 if the file is growing, 0 if it is replaced each period.
      """
      units = self.temporal_pattern_units(fname, self.params['PD'])
      if not units: return 1   # no temporal pattern found in file name    
      if freq[3] > 0:
         if 'H' in units: return 0
      elif freq[2] > 0:
         if 'H' in units or 'D' in units: return 0
      elif freq[1] > 0:
         if 'H' in units or 'D' in units or 'M' in units and not freq[6]: return 0
      elif freq[0] > 0:
         return 0
      return 1

   # add update frequency to date/hour
   # opt = -1 - minus, 0 - begin time, 1 - add (default)
   def addfrequency(self, date, hour, intv, opt = 1):
      """Add (or subtract) an update frequency interval to a date/hour.

      Args:
         date (str | int | None): Starting date (YYYY-MM-DD).
         hour (int | None): Starting hour, or None for daily granularity.
         intv (list | None): Frequency array [Y, M, D, H, min, sec, frac_month].
            Returns (date, hour) unchanged when *intv* is falsy.
         opt (int): 1 to add (default), -1 to subtract, 0 to return the
            beginning of the next period.

      Returns:
         tuple[str, int | None]: Updated (date, hour) pair.
      """
      if date and not isinstance(date, str): date = str(date)
      if not intv: return (date, hour)
      freq = intv.copy()
      if opt == 0: # get begin time of next period
         if freq[3]:
            if freq[3] == 1: return (date, hour)
            (date, hour) = self.adddatehour(date, hour, 0, 0, 0, 1)   # add one hour
         else:
            if freq[2] == 1: return (date, hour)
            date = self.adddate(date, 0, 0, 1)   # add one day
      if opt < 1: # negative frequency for minus
         flen = len(freq)
         for i in range(flen):
            if freq[i]: freq[i] = -freq[i]
      if freq[6]:    # add fraction month
         date = self.addmonth(date, freq[1], freq[6])
      elif hour != None:  # add date/hour
         (date, hour) = self.adddatehour(date, hour, freq[0], freq[1], freq[2], freq[3])
      else:  # add date only
         date = self.adddate(date, freq[0], freq[1], freq[2])
      return (date, hour)

   #  send a customized email if built during specialist's process
   def send_updated_email(self, lindex, locinfo):
      """Send a specialist-built custom email stored in dlupdt.emnote, then clear it.

      Args:
         lindex (int): Local file index whose emnote field to check and send.
         locinfo (str): Log label used if sending fails.
      """
      pgrec = self.pgget("dlupdt", "emnote", "lindex = {}".format(lindex), self.LOGERR)
      if not (pgrec and pgrec['emnote']): return   # no customized email info to send   
      if not self.send_customized_email(locinfo, pgrec['emnote'], self.PGOPT['emllog']): return
      self.pgexec("update dlupdt set emnote = null where lindex = {}".format(lindex), self.LOGERR)   # empty email after sent

   # validate given local indices
   def validate_lindices(self, cact):
      """Validate local file indices in self.params['LI'] against RDADB.

      Converts string values to integers, resolves comparison-operator tokens
      (``!``, ``<``, ``>``) into matching index lists, and verifies ownership.
      Sets a validated flag on self.OPTS['LI'] to avoid re-validation.

      Args:
         cact (str): Current action key (used to enforce -NL for new records).

      Returns:
         int: Count of zero-valued (new-record) indices.
      """
      if (self.OPTS['LI'][2]&8) == 8: return 0  # already validated
      zcnt = 0
      lcnt = len(self.params['LI'])
      i = 0
      while i < lcnt:
         val = self.params['LI'][i]
         if val:
            if isinstance(val, int):
               self.params['LI'][i] = val
            else:
               if re.match(r'^(!|<|>|<>)$', val): break
               self.params['LI'][i] = int(val)
         else:
            self.params['LI'][i] = 0
         i += 1
      if i >= lcnt:   # normal locfile index given
         for i in range(lcnt):
            val = self.params['LI'][i]
            if not val:
               if cact == "SL":
                  if 'NL' not in self.params: self.action_error("Mode option -NL to add new local file record")
                  zcnt += 1
               elif cact == "SR":
                  self.action_error("Local File Index 0 is not allowed\n" +
                               "Use Action SL with Mode option -NL to add new record")
               continue
            if i > 0 and val == self.params['LI'][i-1]: continue
            pgrec = self.pgget("dlupdt", "dsid, specialist", "lindex = {}".format(val), self.PGOPT['extlog'])
            if not pgrec:
               self.action_error("Locfile Index {} is not in RDADB".format(val))
            elif self.OPTS[self.PGOPT['CACT']][2] > 0:
               if pgrec['specialist'] == self.PGLOG['CURUID']:
                  self.params['MD'] = 1
               else:
                  self.validate_dsowner("dsupdt", pgrec['dsid'])
      else: # found none-equal condition sign
         pgrec = self.pgmget("dlupdt", "DISTINCT lindex", self.get_field_condition("lindex", self.params['LI'], 0, 1), self.PGOPT['extlog'])
         if not pgrec: self.action_error("No update record matches given Locfile Index condition")
         self.params['LI'] = pgrec['lindex']
      self.OPTS['LI'][2] |= 8   # set validated flag
      return zcnt

   # validate given control indices
   def validate_cindices(self, cact):
      """Validate update control indices in self.params['CI'] (or 'ID') against RDADB.

      Converts string values to integers, resolves comparison/wildcard tokens
      into matching index lists, verifies that each index exists, and enforces
      single-control-per-invocation for update/check actions.  Falls back to
      cid2cindex() when 'ID' is provided instead of 'CI'.

      Args:
         cact (str): Current action key.

      Returns:
         int: Count of zero-valued (new-control) indices.
      """
      if (self.OPTS['CI'][2] & 8) == 8: return 0   # already validated
      zcnt = 0
      if 'CI' in self.params:
         ccnt = len(self.params['CI'])
         i = 0
         while i < ccnt:
            val = self.params['CI'][i]
            if val:
               if isinstance(val, int):
                  self.params['CI'][i] = val
               else:
                  if re.match(r'^(!|<|>|<>)$', val): break
                  self.params['CI'][i] = int(val)
            else:
               self.params['CI'][i] = 0
            i += 1
         if i >= ccnt:   # normal locfile index given
            for i in range(ccnt):
               val = self.params['CI'][i]
               if not val:
                  if cact == 'SC':
                     if 'NC' in self.params:
                        self.params['CI'][i] = 0
                        zcnt += 1
                     else:
                        self.action_error("Mode option -NC to add new update control record")
                  continue
               if i > 0 and val == self.params['CI'][i-1]: continue
               pgrec = self.pgget("dcupdt", "dsid, specialist", "cindex = {}".format(val), self.PGOPT['extlog'])
               if not pgrec:
                  self.action_error("Control Index {} is not in RDADB".format(val))
               elif self.OPTS[self.PGOPT['CACT']][2] > 0:
                  if pgrec['specialist'] == self.PGLOG['CURUID']:
                     self.params['MD'] = 1
                  else:
                     self.validate_dsowner("dsupdt", pgrec['dsid'])
         else: # found none-equal condition sign
            pgrec = self.pgmget("dcupdt", "DISTINCT cindex", self.get_field_condition("cindex", self.params['CI'], 0, 1), self.PGOPT['extlog'])
            if not pgrec: self.action_error("No update control record matches given Index condition")
            self.params['CI'] = pgrec['cindex']   
         if len(self.params['CI']) > 1 and self.PGOPT['ACTS']&self.PGOPT['CNTLACTS']:
            self.action_error("Process one Update Control each time")
      elif 'ID' in self.params:
         self.params['CI'] = self.cid2cindex(cact, self.params['ID'], zcnt)
      self.OPTS['CI'][2] |= 8   # set validated flag
      return zcnt

   # get control index array from given control IDs
   def cid2cindex(self, cact, cntlids, zcnt):
      """Convert a list of control ID strings to their corresponding cindex values.

      Performs an exact-match DB lookup for normal IDs and a wildcard/comparison
      query for patterns containing ``%``, ``!``, ``<``, or ``>``.

      Args:
         cact (str): Current action key (used to allow new-record creation).
         cntlids (list[str]): List of control ID strings from self.params['ID'].
         zcnt (int): Running count of zero-index (new-record) placeholders.

      Returns:
         list[int]: List of resolved cindex integers.
      """
      count = len(cntlids) if cntlids else 0
      if count == 0: return None
      i = 0
      while i < count:
         val = cntlids[i]
         if val and (re.match(r'^(!|<|>|<>)$', val) or val.find('%') > -1): break
         i += 1
      if i >= count: # normal control id given
         indices = [0]*count
         for i in range(count):
            val = cntlids[i]
            if not val:
               continue
            elif i and (val == cntlids[i-1]):
               indices[i] = indices[i-1]
               continue
            else:
               pgrec = self.pgget("dcupdt", "cindex", "cntlid = '{}'".format(val), self.PGOPT['extlog'])
               if pgrec: indices[i] = pgrec['cindex']
            if not indices[i]:
               if cact == "SC":
                  if 'NC' in self.params:
                     indices[i] = 0
                     zcnt += 1
                  else:
                     self.action_error("Control ID {} is not in RDADB,\n".format(val) +
                                       "Use Mode Option -NC (-NewControl) to add new Control", cact)
               else:
                  self.action_error("Control ID '{}' is not in RDADB".format(val), cact)
         return indices
      else:  # found wildcard and/or none-equal condition sign
         pgrec = self.pgmget("dcupdt", "DISTINCT cindex",  self.get_field_condition("cntlid", cntlids, 1, 1), self.PGOPT['extlog'])
         if not pgrec: self.action_error("No Control matches given Control ID condition")
         return pgrec['cindex']

   # check remote server file information
   def check_server_file(self, dcmd, opt, cfile):
      """Retrieve metadata for the source file referenced by a download command.

      Parses the command to identify the transfer protocol (rdacp, cp/mv, tar,
      ncftpget, wget, or generic) and delegates to the appropriate
      check_*_file() helper.  The returned dict includes a 'ftype' key
      indicating the protocol detected.

      Args:
         dcmd (str): Download command string.
         opt (int): Option bitmask passed to the underlying check helper.
         cfile (str | None): Comparison file path for wget-based newer checks.

      Returns:
         dict | None: File info dict (data_size, date_modified, etc.) or None.
      """
      sfile = info = type = None
      self.PGLOG['SYSERR'] = self.PGOPT['STATUS'] = ''
      docheck = 1
      copt = opt|256
      ms = re.search(r'(^|\s|\||\S/)rdacp\s+(.+)$', dcmd)
      if ms:
         buf = ms.group(2)
         type = "RDACP"
         docheck = 0
         ms = re.match(r'^(-\w+)', buf)
         while ms:
            flg = ms.group(1)
            buf = re.sub(r'^-\w+\s+'.format(flg), '', buf, 1)   # remove options
            if flg != "-r":   # has option value
               m = re.match(r'^(\S+)\s', buf)
               if not m: break
               if flg == "-f":
                  sfile = m.group(1)
               elif flg == "-fh":
                  target = m.group(1)
               buf = re.sub(r'^\S+\s+', '', buf, 1)   # remove values
            ms = re.match(r'^(-\w+)', buf)
         if not sfile:
             ms = re.match(r'^(\S+)', buf)
             if ms: sfile = ms.group(1)
         info = self.check_rda_file(sfile, target, copt)
      if docheck:
         ms = re.search(r'(^|\s|\||\S/)(mv|cp)\s+(.+)$', dcmd)
         if ms:
            sfile = ms.group(3)
            type = "COPY" if ms.group(2) == "cp" else "MOVE"
            docheck = 0
            ms = re.match(r'^(-\w+\s+)', sfile)
            while ms:
               sfile = re.sub(r'^-\w+\s+', '', sfile, 1)   # remove options
               ms = re.match(r'^(-\w+\s+)', sfile)
            ms = re.match(r'^(\S+)\s', sfile)
            if ms: sfile = ms.group(1)
            info = self.check_local_file(sfile, copt)
      if docheck:
         ms = re.search(r'(^|\s|\||\S/)tar\s+(-\w+)\s+(\S+\.tar)\s+(\S+)$', dcmd)
         if ms:
            sfile = ms.group(4)
            target = ms.group(3)
            type = "UNTAR" if ms.group(2).find('x') > -1 else "TAR"
            docheck = 0
            info = self.check_tar_file(sfile, target, copt)
      if docheck:
         ms = re.search(r'(^|\s|\||\S/)ncftpget\s(.*)(ftp://\S+)', dcmd, re.I)
         if ms:
            sfile = ms.group(3)
            buf = ms.group(2)
            type = "FTP"
            docheck = 0
            user = pswd = None
            if buf:
               ms = re.search(r'(-u\s+|--user=)(\S+)', buf)
               if ms: user = ms.group(2)
               ms = re.search(r'(-p\s+|--password=)(\S+)', buf)
               if ms: pswd = ms.group(2)
            info = self.check_ftp_file(sfile, copt, user, pswd)
      if docheck:
         ms = re.search(r'(^|\s|\||\S/)wget(\s.*)https{0,1}://(\S+)', dcmd, re.I)
         if ms:
            obuf = ms.group(2)
            wbuf = ms.group(3)
            sfile = op.basename(wbuf)
            self.slow_web_access(wbuf)
            type = "WGET"
            docheck = 0
            if not obuf or not re.search(r'\s-N\s', obuf): dcmd = re.sub(r'wget', 'wget -N', dcmd, 1)
            flg = 0
            if cfile and sfile != cfile:
               if self.pgsystem("cp -p {} {}".format(cfile, sfile), self.PGOPT['emerol'], 4): flg = 1
            buf = self.pgsystem(dcmd, self.PGOPT['wrnlog'], 16+32)
            info = self.check_local_file(sfile, opt, self.PGOPT['wrnlog'])
            if buf:
               if not info: self.PGOPT['STATUS'] = buf
               if re.search(r'Saving to:\s', buf):
                  flg = 0
               elif not re.search(r'(Server file no newer|not modified on server)', buf):
                  if info: info['note'] = "{}:\n{}".format(dcmd, buf)
            else:
               if info: info['note'] = dcmd + ": Failed checking new file"   
            if flg: self.pgsystem("rm -rf " + sfile, self.PGOPT['emerol'], 4)
      if docheck:
         ms = re.match(r'^(\S+)\s+(.+)$', dcmd)
         if ms:
            buf = ms.group(2)
            type = op.basename(ms.group(1)).upper()
            files = re.split(' ', buf)
            for file in files:
               if re.match(r'^-\w+', file) or not op.exists(file) or cfile and file == cfile: continue
               info = self.check_local_file(file, copt)
               if info:
                  info['data_size'] = 0
                  break   
               sfile = file
      if info:
         info['ftype'] = type
      else:
         if not self.PGOPT['STATUS']: self.PGOPT['STATUS'] = self.PGLOG['SYSERR']
         if not sfile: self.pglog(dcmd + ": NO enough information in command to check file info", self.PGOPT['errlog'])
      return info

   # check and sleep if given web site need to be slowdown for accessing
   def slow_web_access(self, wbuf):
      """Sleep to rate-limit access to web servers that require throttling.

      Checks *wbuf* against the self.WSLOWS dictionary (hostname → sleep
      seconds) and sleeps the configured number of seconds when a match is found.

      Args:
         wbuf (str): URL or command string that may contain a known slow hostname.
      """
      for wsite in self.WSLOWS:
         if wbuf.find(wsite) > -1:
            time.sleep(self.WSLOWS[wsite])

   # check remote server/file status information
   # return 1 if exists; 0 missed, -1 with error, -2 comand not surported yet
   # an error message is stored in self.PGOPT['STATUS'] if not success
   def check_server_status(self, dcmd):
      """Check whether the remote source file referenced by a download command exists.

      Dispatches to the appropriate protocol handler (rdacp, mv/cp/tar,
      ncftpget, wget, or generic) based on the command content.

      Args:
         dcmd (str): Download command string.

      Returns:
         int: 1 if the file exists, 0 if not found (possibly just not ready),
         -1 on a recoverable error, -2 on an unrecoverable error or unsupported
         command.
      """
      self.PGOPT['STATUS'] = ''
      target = None
      ms = re.search(r'(^|\s|\||\S/)rdacp\s+(.+)$', dcmd)
      if ms:
         buf = ms.group(2)
         ms = re.search(r'-fh\s+(\S+)', buf)
         if ms: target = ms.group(1)
         ms = re.search(r'-f\s+(\S+)', buf)
         if ms:
            fname = ms.group(1)
         else:
            ms = re.match(r'^(-\w+)', buf)
            while ms:
               flg = ms.group(1)
               buf = re.sub(r'^-\w+\s+', '', buf, 1)   # remove options
               if flg != "-r":   # no option value
                  if not re.match(r'^\S+\s', buf): break
                  buf = re.sub(r'^\S+\s+', '', buf, 1)   # remove values
               ms = re.match(r'^(-\w+)', buf)
            ms = re.match(r'^(\S+)', buf)
            if ms: fname = ms.group(1)
            if not fname:
               self.PGOPT['STATUS'] = dcmd + ": MISS from-file per option -f"
               return -1
         if not target:
            return self.check_local_status(fname)
         else:
            return self.check_remote_status(target, fname)
      ms = re.search(r'(^|\s|\||\S/)(mv|cp|tar|cnvgrib|grabbufr|pb2nc)\s+(.+)$', dcmd)
      if ms:
         buf = ms.group(2)
         fname = ms.group(3)
         ms = re.match(r'^(-\w+\s+)', fname)
         while ms:
            fname = re.sub(r'^-\w+\s+', '', fname, 1)   # remove options
            ms = re.match(r'^(-\w+\s+)', fname)
         ms = re.match(r'^(\S+)\s+(\S*)', fname)
         if ms:
            fname = ms.group(1)
            if buf == 'tar': target = ms.group(2)
         if target:
            return self.check_tar_status(fname, target)
         else:
            return self.check_local_status(fname)
      ms = re.search(r'(^|\s|\||\S/)ncftpget\s(.*)(ftp://[^/]+)(/\S+)', dcmd, re.I)
      if ms:
         buf = ms.group(2)
         target = ms.group(3)
         fname = ms.group(4)
         user = pswd = None
         if buf:
            ms = re.search(r'(-u\s+|--user=)(\S+)', buf)
            if ms: user = ms.group(2)
            ms = re.search(r'(-p\s+|--password=)(\S+)', buf)
            if ms: pswd = ms.group(2)
         return self.check_ftp_status(target, fname, user, pswd)
      ms = re.search(r'(^|\s|\||\S/)wget\s(.*)(https{0,1}://[^/]+)(/\S+)', dcmd, re.I)
      if ms:
         buf = ms.group(2)
         target = ms.group(3)
         fname = ms.group(4)
         user = pswd = None
         if buf:
            ms = re.search(r'(-u\s+|--user=|--http-user=)(\S+)', buf)
            if ms: user = ms.group(2)
            ms = re.search(r'(-p\s+|--password=|--http-passwd=)(\S+)', buf)
            if ms: pswd = ms.group(2)
         return self.check_wget_status(target, fname, user, pswd)
      ms = re.match(r'^\s*(\S+)', dcmd)
      if ms and self.valid_command(ms.group(1)):
         return 0
      else:
         self.PGOPT['STATUS'] = dcmd + ": Invalid command"
      return -2

   # check status for remote server/file via wget
   # return self.SUCCESS if file exist and self.FAILURE otherwise.
   # file status message is returned via reference string of $status
   def check_wget_status(self, server, fname, user, pswd):
      """Check the existence of a remote file on an HTTP/HTTPS server using wget --spider.

      Walks up the directory tree (up to self.PGOPT['PCNT'] levels) if the
      exact path is not found, to distinguish a missing file from an
      inaccessible server.

      Args:
         server (str): Base server URL (e.g. ``https://example.com``).
         fname (str): File path on the server.
         user (str | None): HTTP username (wget --user).
         pswd (str | None): HTTP password (wget --password).

      Returns:
         int: 1 if found, 0 if not yet present (parent dir exists),
         -1 if file is missing, -2 on a server/network error.
      """
      cmd = "wget --spider --no-check-certificate "
      if user or pswd:
         self.PGOPT['STATUS'] = "{}{}: {}".format(server, fname, self.PGLOG['MISSFILE'])
         return -1
      if user: cmd += "--user={} ".format(user)
      if pswd: cmd += "--password={} ".format(pswd)
      cmd += server
      pname = None
      i = 0
      while True:
         msg = self.pgsystem(cmd + fname, self.LOGWRN, 48)   # 16+32
         if msg:
            if msg.find('Remote file exists') > -1:
               if pname:
                  self.PGOPT['STATUS'] = "{}{}: {}".format(server, pname, self.PGLOG['MISSFILE'])
                  return (-1 if i > self.PGOPT['PCNT'] else 0)
               else:
                  return 1
            elif msg.find('unable to resolve host address') > -1:
               self.PGOPT['STATUS'] = server + ": Server Un-accessible"
               return -2
            elif msg.find('Remote file does not exist') < 0:
               self.PGOPT['STATUS'] = "{}{}: Error check status:\n{}".format(cmd, fname, msg)
               return -2
         pname = fname
         fname = op.dirname(pname)
         if not fname or fname == "/":
            self.PGOPT['STATUS'] = "{}{}: {}".format(server, pname, self.PGLOG['MISSFILE'])
            return -1
         fname += "/"
         i += 1

   # check status for remote server/file via check_ftp_file()
   # return self.SUCCESS if file exist and self.FAILURE otherwise.
   # file status message is returned via reference string of $status
   def check_ftp_status(self, server, fname, user, pswd):
      """Check the existence of a remote file on an FTP server using ncftpls.

      Walks up the directory tree on missing entries to distinguish a missing
      file from an inaccessible parent directory or server.

      Args:
         server (str): FTP server URL (e.g. ``ftp://data.example.com``).
         fname (str): File path on the server.
         user (str | None): FTP username.
         pswd (str | None): FTP password.

      Returns:
         int: 1 if found, 0 if not yet present, -1 if file is missing,
         -2 on an unrecoverable server error.
      """
      cmd = "ncftpls "
      if user: cmd += "-u {} ".format(user)
      if pswd: cmd += "-p {} ".format(pswd)
      cmd += server
      pname = None
      i = 0
      while True:
         msg = self.pgsystem(cmd + fname, self.LOGWRN, 272)   # 16+256
         if self.PGLOG['SYSERR']:
            if self.PGLOG['SYSERR'].find('unknown host') > -1:
               self.PGOPT['STATUS'] = server + ": Server Un-accessible"
               return -2
            elif self.PGLOG['SYSERR'].find('Failed to change directory') < 0:
               self.PGOPT['STATUS'] = "{}{}: Error check status:\n{}".format(server, fname, self.PGLOG['SYSERR'])
               return -2
         elif not msg:
            self.PGOPT['STATUS'] = "{}{}: {}".format(server, fname, self.PGLOG['MISSFILE'])
            return -1 if i >= self.PGOPT['PCNT'] else 0
         elif pname:
            self.PGOPT['STATUS'] = "{}{}: {}".format(server, pname, self.PGLOG['MISSFILE'])
            return -1 if i > self.PGOPT['PCNT'] else 0
         else:
            return 1
         pname = fname
         fname = op.dirname(pname)
         if not fname or fname == "/":
            self.PGOPT['STATUS'] = "{}{}: {}".format(server, pname, self.PGLOG['MISSFILE'])
            return -1
         i += 1

   # check remote server status
   def check_remote_status(self, host, fname):
      """Check the existence of a file on an RDA remote host via ``<host>-sync``.

      Walks up the path tree to distinguish a missing file from an inaccessible
      parent directory.

      Args:
         host (str): Remote host prefix used to construct ``<host>-sync``.
         fname (str): File path to check on the remote host.

      Returns:
         int: 1 if found, 0 if not yet present, -1 if file is missing,
         -2 on error.
      """
      pname = None
      i = 0
      while True:
         msg = self.pgsystem("{}-sync {}".format(host, fname), self.LOGWRN, 272)   # 16+256
         if msg:
            for line in re.split('\n', msg):
               info = self.remote_file_stat(line, 0)
               if info:
                  if pname:
                     self.PGOPT['STATUS'] = "{}-{}: {}".format(host, pname. self.PGLOG['MISSFILE'])
                     return -1 if i > self.PGOPT['PCNT'] else 0
                  else:
                     return 1
         if self.PGLOG['SYSERR'] and self.PGLOG['SYSERR'].find(self.PGLOG['MISSFILE']) < 0:
            self.PGOPT['STATUS'] = "{}-sync {}: Error check status:\n{}".format(host, fname, self.PGLOG['SYSERR'])
            return -2
         pname = fname
         fname = op.dirname(pname)
         if not fname or fname == "/":
            self.PGOPT['STATUS'] = "{}-{}: {}".format(host, pname, self.PGLOG['MISSFILE'])
            return -1
         i += 1

   # check local disk status
   def check_local_status(self, fname):
      """Check the existence of a path on the local filesystem.

      Walks up the directory tree to distinguish a missing file from a missing
      parent directory, returning different codes for each case.

      Args:
         fname (str): Absolute or relative path to check.

      Returns:
         int: 1 if path exists, 0 if parent exists but file does not,
         -1 if file is missing, -2 on error.
      """
      pname = None
      i = 0
      while True:
         if op.exists(fname):
            if pname:
               self.PGOPT['STATUS'] = "{}: {}".format(pname, self.PGLOG['MISSFILE'])
               return -1 if i > self.PGOPT['PCNT'] else 0
            else:
               return 1
         if self.PGLOG['SYSERR'] and self.PGLOG['SYSERR'].find(self.PGLOG['MISSFILE']) < 0:
            self.PGOPT['STATUS'] = "{}: Error check status:\n{}".format(fname, self.PGLOG['SYSERR'])
            return -2
         pname = fname
         fname = op.dirname(pname)
         if not fname or fname == "/":
            self.PGOPT['STATUS'] = "{}: {}".format(pname, self.PGLOG['MISSFILE'])
            return -1
         i += 1

   # check tar file status
   def check_tar_status(self, fname, target):
      """Check whether *target* exists inside a local tar archive *fname*.

      Args:
         fname (str): Path to the tar file.
         target (str): Member path to look for within the archive.

      Returns:
         int: 1 if the member exists, 0 if the archive exists but the member
         does not, -1 on error.
      """
      stat = self.check_local_status(fname)
      if stat < 1: return stat
      msg = self.pgsystem("tar -tvf {} {}".format(fname, target), self.LOGWRN, 272)   # 16+256
      if msg:
         for line in re.split('\n', msg):
            if self.tar_file_stat(line, 0): return 1
      if not self.PGLOG['SYSERR'] or self.PGLOG['SYSERR'].find('Not found in archive') > -1:
         self.PGOPT['STATUS'] = "{}: Not found in tar file {}".format(target, fname)
         return 0
      else:
         self.PGOPT['STATUS'] = "{}: Error check tar file {}:\n{}".format(target, fname, self.PGLOG['SYSERR'])
         return -1

   # count directories with temoral patterns in given path
   def count_pattern_path(self, dcmd):
      """Count the number of path components containing temporal patterns in a command.

      Used to determine how many directory levels to walk up when checking
      file existence on a remote server.

      Args:
         dcmd (str): Download command string.

      Returns:
         int: Number of pattern-containing path levels (minimum 1).
      """
      getpath = 1
      ms = re.search(r'(^|\s|\||\S/)rdacp\s+(.+)$', dcmd)
      if ms:
         path = ms.group(2)
         getpath = 0
         ms = re.search(r'-f\s+(\S+)', path)
         if ms:
            path = ms.group(1)
         else:
            ms = re.match(r'^(-\w+)', path)
            while ms:
               flg = ms.group(1)
               path = re.sub(r'^-\w+\s+', '', path, 1)   # remove options
               if flg != "-r":    # no option value
                  ms = re.match(r'^(\S+)\s', path)
                  if not ms: break
                  path = re.sub(r'^\S+\s+', '', path, 1)    # remove values
               ms = re.match(r'^(-\w+)', path)
            ms = re.match(r'^(\S+)', path)
            if ms: path = ms.group(1)
            if not path: return self.pglog(dcmd + ": MISS from-file per option -f", self.PGOPT['emlerr'])
      if getpath:
         ms = re.search(r'(^|\s|\||\S/)(mv|cp|tar|cnvgrib|grabbufr|pb2nc)\s+(.+)$', dcmd)
         if ms:
            path = ms.group(3)
            getpath = 0
            ms = re.match(r'^-\w+\s', path)
            while ms:
               path = re.sub(r'^-\w+\s+', '', path, 1)   # remove options
               ms = re.match(r'^-\w+\s', path)
            ms = re.match(r'^(\S+)\s+(\S*)', path)
            if ms: path = ms.group(1)
      if getpath:
         ms = re.search(r'(^|\s|\||\S/)(ncftpget|wget)\s(.*)(ftp|http|https)://[^/]+(/\S+)', dcmd, re.I)
         if ms: path = ms.group(5)
      if not path: return self.pglog(dcmd + ": Unkown command to count pattern path", self.PGOPT['emlerr'])
      pcnt = path.find(self.params['PD'][0])
      if pcnt > 0:
         path = path[pcnt:]
         p = re.findall(r'/', path)
         pcnt = len(p) + 1
      else:
         pcnt = 1
      return pcnt

   # check error message for download action
   def parse_download_error(self, err, act, sinfo = None):
      """Classify a download error message and determine whether to retry or abort.

      Returns a (status, error_message) tuple where status follows the
      convention: 1 = success, 0 = recoverable missing, -1 = error continue,
      -2 = unrecoverable error.

      Args:
         err (str): Stderr output from the download command.
         act (str): Download action name (e.g. 'wget', 'ncftpget', 'UNTAR').
         sinfo (dict | None): File info dict from check_local_file(); if
            provided, size validation takes precedence over *err*.

      Returns:
         tuple[int, str]: (status_code, cleaned_error_message).
      """
      derr = ''
      stat = 0
      if sinfo:
         if sinfo['data_size'] == 0:
            derr = ", empty file"
            if err: derr += ' ' + err
         elif sinfo['data_size'] < self.PGLOG['MINSIZE']:
            derr = ", small file({}B)".format(sinfo['data_size'])
            if err: derr += ' ' + err
         else:
            stat = 1
      elif err:
         derr = err
         if (err.find('command not found') > -1 or
             err.find('403 Forbidden') > -1):
            stat = -2
         elif (act == "wget" and err.find('404 Not Found') > -1 or
             act == "UNTAR" and err.find('Not found in archive') > -1 or
             act == "ncftpget" and err.find('Failed to open file') > -1 or
             err.find(self.PGLOG['MISSFILE']) > -1):
            derr = self.PGLOG['MISSFILE']
         else:
            stat = -1
      return (stat, derr)

   # cache update control information
   def cache_update_control(self, cidx, dolock = 0):
      """Load a dcupdt control record into self.PGOPT['UCNTL'] and apply its settings.

      Validates the dataset ID, hostname restrictions, and data time.  Applies
      control flags (updtcntl, emailcntl, errorcntl, keepfile) to self.params,
      caches associated data times, and optionally acquires an exclusive lock.

      Args:
         cidx (int): Control index (dcupdt.cindex) to cache.
         dolock (int): If non-zero, acquire an exclusive update-control lock.

      Returns:
         bool: self.SUCCESS on success, self.FAILURE on any validation error.
      """
      cstr = "C{}".format(cidx)
      pgrec = self.pgget("dcupdt", "*", "cindex = {}".format(cidx), self.PGOPT['emlerr'])
      if not pgrec: return self.pglog(cstr + ": update control record NOT in RDADB", self.PGOPT['errlog'])
      if pgrec['dsid']:
         if 'DS' not in self.params: self.params['DS'] = pgrec['dsid']
         cstr = "{}-{}".format(self.params['DS'], cstr)
         if self.params['DS'] != pgrec['dsid']:
            return self.pglog("{}: Control dataset {} NOT match".format(cstr, pgrec['dsid']), self.PGOPT['emlerr'])
      if pgrec['hostname'] and not self.valid_control_host(cstr, pgrec['hostname'], self.PGOPT['emlerr']): return self.FAILURE
      if not ('ED' in self.params or self.valid_data_time(pgrec, cstr, self.PGOPT['emlerr'])): return self.FAILURE
      if dolock and self.lock_update_control(cidx, 1, self.PGOPT['emlerr']) <= 0: return self.FAILURE
      if self.PGLOG['DSCHECK']: self.set_dscheck_attribute("oindex", cidx)
      if pgrec['updtcntl']:
         if pgrec['updtcntl'].find('A') > -1: self.params['CA'] = 1
         if pgrec['updtcntl'].find('B') > -1: self.params['UB'] = 1
         if pgrec['updtcntl'].find('C') > -1: self.params['CP'] = 1
         if pgrec['updtcntl'].find('E') > -1: self.params['RE'] = 1
         if pgrec['updtcntl'].find('F') > -1: self.params['FU'] = 1
         if pgrec['updtcntl'].find('G') > -1:
            self.params['GZ'] = 1
            self.PGLOG['GMTZ'] = self.diffgmthour()
         if pgrec['updtcntl'].find('M') > -1: self.params['MU'] = 1
         if pgrec['updtcntl'].find('N') > -1: self.params['CN'] = 1
         if pgrec['updtcntl'].find('O') > -1: self.params['MO'] = 1
         if pgrec['updtcntl'].find('Y') > -1: self.PGLOG['NOLEAP'] = self.params['NY'] = 1
         if pgrec['updtcntl'].find('Z') > -1 and 'VS' not in self.params:
            self.PGLOG['MINSIZE'] = self.params['VS'] = 0
      if pgrec['emailcntl'] != 'A':
         if pgrec['emailcntl'] == "N":
            self.params['NE'] = 1
            self.PGLOG['LOGMASK'] &= ~self.EMLALL   # turn off all email acts
         elif pgrec['emailcntl'] == "S":
            self.params['SE'] = 1
            self.PGOPT['emllog'] |= self.EMEROL
         elif pgrec['emailcntl'] == "E":
            self.params['EE'] = 1
         elif pgrec['emailcntl'] == "B":
            self.params['SE'] = 1
            self.params['EE'] = 1
            self.PGOPT['emllog'] |= self.EMEROL
      if pgrec['errorcntl'] != 'N':
         if pgrec['errorcntl'] == "I":
            self.params['IE'] = 1
         elif pgrec['errorcntl'] == "Q":
            self.params['QE'] = 1
      if pgrec['keepfile'] != 'N':
         if pgrec['keepfile'] == "S":
            self.params['KS'] = 1
         elif pgrec['keepfile'] == "R":
            self.params['KR'] = 1
         elif pgrec['keepfile'] == "B":
            self.params['KR'] = 1
            self.params['KS'] = 1
      if pgrec['houroffset'] and 'HO' not in self.params: self.params['HO'] = [pgrec['houroffset']]
      if pgrec['emails'] and 'CC' not in self.params: self.add_carbon_copy(pgrec['emails'], 1)
      self.cache_data_time(cidx)
      self.PGOPT['UCNTL'] = pgrec
      return self.SUCCESS

   # cache date time info
   def cache_data_time(self, cidx):
      """Pre-populate self.PGOPT['DTIMES'] with enddate/endhour for all local files
      associated with control index *cidx*.

      Args:
         cidx (int): Control index whose dlupdt records to cache.
      """
      pgrecs = self.pgmget("dlupdt", "lindex, enddate, endhour", "cindex = {}".format(cidx), self.PGOPT['emlerr'])
      cnt =  len(pgrecs['lindex']) if pgrecs else 0
      for i in range(cnt):
         if not pgrecs['enddate'][i]: continue
         dhour = pgrecs['endhour'][i] if (pgrecs['endhour'][i] is not None) else 23
         self.PGOPT['DTIMES'][pgrecs['lindex'][i]] = "{} {:02}:59:59".format(pgrecs['enddate'][i], dhour)

   #  check if valid host to process update control
   def valid_control_host(self, cstr, hosts, logact):
      """Verify that the current host is allowed to process the given update control.

      The *hosts* field may be a plain hostname (must match) or ``!hostname``
      (must NOT match).

      Args:
         cstr (str): Control identifier string for log messages.
         hosts (str | None): Allowed or excluded hostname pattern from dcupdt.
         logact (int): Log action bitmask for failure messages.

      Returns:
         bool: self.SUCCESS if the host is valid, self.FAILURE otherwise.
      """
      host = self.get_host(1)
      if hosts:
         if re.search(host, hosts, re.I):
            if hosts[0] == '!':
               return self.pglog("{}: CANNOT be processed on {}".format(cstr, hosts[1:]), logact)
         elif not re.match(r'^!', hosts):
            return self.pglog("{}-{}: MUST be processed on {}".format(host, cstr, hosts), logact)
      return self.SUCCESS

   # reset updated data time
   def reset_data_time(self, qu, ddate, dhour, lidx):
      """Update dcupdt.datatime and chktime to reflect the latest successfully archived data.

      Tracks the earliest data time across all local files in PGOPT['DTIMES']
      and writes the minimum value to the control record if it has advanced.

      Args:
         qu (str | None): Update frequency unit ('H', 'D', etc.) to determine
            default hour when *dhour* is None.
         ddate (str | None): Data date of the archived file.
         dhour (int | None): Data hour of the archived file.
         lidx (int): Local file index being updated.
      """
      pgrec = self.PGOPT['UCNTL']
      record = {'chktime': int(time.time())}
      if ddate:
         if dhour is None: dhour = 0 if qu == 'H' else 23
         dtime = "{} {:02}:59:59".format(ddate, dhour)
         if lidx not in self.PGOPT['DTIMES'] or self.pgcmp(self.PGOPT['DTIMES'][lidx], dtime) < 0:
            self.PGOPT['DTIMES'][lidx] = dtime
         # get earliest data time
         for ltime in self.PGOPT['DTIMES'].values():
            if self.pgcmp(ltime, dtime) < 0: dtime = ltime
         if not pgrec['datatime'] or self.pgcmp(pgrec['datatime'], dtime) < 0:
            self.PGOPT['UCNTL']['datatime'] = record['datatime'] = dtime
      if self.pgupdt("dcupdt", record, "cindex = {}".format(pgrec['cindex']), self.PGOPT['extlog']) and 'datatime' in record:
          self.pglog("{}-C{}: Data time updated to {}".format(self.params['DS'], pgrec['cindex'], dtime),  self.PGOPT['emllog'])

   # adjust control time according to the control offset
   def adjust_control_time(self, cntltime, freq, unit, offset, curtime):
      """Advance a control timestamp to the next scheduled run time.

      Removes any configured offset, aligns the time to the frequency boundary
      (e.g. truncates to the start of an hour or period), re-applies the offset,
      then increments by *freq* until the result is strictly after *curtime*.

      Args:
         cntltime (str): Current dcupdt.cntltime (``YYYY-MM-DD HH:MM:SS``).
         freq (list): Frequency array from get_control_time().
         unit (str): Frequency unit character ('H', 'D', 'M', 'Y', 'W').
         offset (str | None): Control offset string (e.g. '2H') or None.
         curtime (str): Current wall-clock datetime string.

      Returns:
         str: Next scheduled control time (``YYYY-MM-DD HH:MM:SS``).
      """
      if offset:
         ofreq = self.get_control_time(offset, "Control Offset")
         if ofreq:   # remove control offset
            nfreq = ofreq.copy()
            for i in range(6):
               if nfreq[i]: nfreq[i] = -nfreq[i]
            cntltime = self.adddatetime(cntltime, nfreq[0], nfreq[1], nfreq[2], nfreq[3], nfreq[4], nfreq[5], nfreq[6])
      else:
         ofreq = None
      (cdate, ctime) = re.split(' ', cntltime)
      if unit == "H":
         hr = 0
         if ctime:
            ms = re.match(r'^(\d+)', ctime)
            if ms: hr = int(int(ms.group(1))/freq[3])*freq[3]
         else:
            i = 0
         cntltime = "{} {:02}:00:00".format(cdate, hr)
      else:
         cdate = self.enddate(cdate, (0 if unit == "W" else 1), unit, freq[6])
         cntltime = "{} 00:00:00".format(cdate)
      if ofreq: cntltime = self.adddatetime(cntltime, ofreq[0], ofreq[1], ofreq[2], ofreq[3], ofreq[4], ofreq[5], ofreq[6])   # add control offset
      while self.pgcmp(cntltime, curtime) <= 0:
         cntltime = self.adddatetime(cntltime, freq[0], freq[1], freq[2], freq[3], freq[4], freq[5], freq[6])
      return cntltime

   #  reset control time
   def reset_control_time(self):
      """Compute and write the next dcupdt.cntltime after a completed (or failed) run.

      If there were errors, the retry interval is used to schedule an earlier
      retry provided it falls before the normal next run time.
      """   
      pgrec = self.PGOPT['UCNTL']
      cstr = "{}-C{}".format(self.params['DS'], pgrec['cindex'])
      gmt = self.PGLOG['GMTZ']
      self.PGLOG['GMTZ'] = 0
      curtime = self.curtime(1)
      self.PGLOG['GMTZ'] = gmt
      (freq, unit) = self.get_control_frequency(pgrec['frequency'])
      if not freq: return self.pglog("{}: {}".format(cstr, unit), self.PGOPT['emlerr'])
      cntltime = self.check_datetime(pgrec['cntltime'], curtime)
      nexttime = self.adjust_control_time(cntltime, freq, unit, pgrec['cntloffset'], curtime)
      if self.PGLOG['ERRCNT']:
         cfreq = self.get_control_time(pgrec['retryint'], "Retry Interval")
         if cfreq:
            while self.pgcmp(cntltime, curtime) <= 0:
               cntltime = self.adddatetime(cntltime, cfreq[0], cfreq[1], cfreq[2], cfreq[3], cfreq[4], cfreq[5], cfreq[6])
            if self.pgcmp(cntltime, nexttime) < 0: nexttime = cntltime
      record = {}
      cstr += ": Next Control Time "
      if not pgrec['cntltime'] or self.pgcmp(nexttime, pgrec['cntltime']) > 0:
         record['cntltime'] = nexttime
         cstr += "set to {}".format(nexttime)
         if self.PGLOG['ERRCNT']: cstr += " to retry"
      else:
         cstr += "already set to {}".format(pgrec['cntltime'])
      cstr += " for Action {}({})".format(self.PGOPT['CACT'], self.OPTS[self.PGOPT['CACT']][1])
      record['pid'] = 0
      if self.pgupdt("dcupdt", record, "cindex = {}".format(pgrec['cindex']), self.PGOPT['extlog']):
         self.pglog(cstr, self.PGOPT['emllog'])

   # get array information of individual controlling time
   def get_control_time(self, val, type):
      """Parse a time-interval string into a 7-element integer array.

      Recognises Y (year), M (month), D (day), W (week, converted to days),
      H (hour), N (minute), and S (second) suffixes.  Returns 0 for falsy or
      '0' values.  Logs and returns None for unsupported fractional notation or
      strings with no recognised units.

      Args:
         val (str | None): Interval string such as ``'6H'``, ``'1M'``, ``'2W3D'``.
         type (str): Human-readable name for error messages.

      Returns:
         list[int] | int | None: 7-element list [Y,M,D,H,min,sec,frac], 0 for
         empty input, or None on parse error.
      """
      if not val or val == '0': return 0
      if re.search(r'/(\d+)$', val):
         return self.pglog("{}: '{}' NOT support Fraction".format(val, type), self.PGOPT['emlerr'])
      ctimes = [0]*7   # initialize control times
      ms = re.search(r'(\d+)Y', val, re.I)
      if ms: ctimes[0] = int(ms.group(1))
      ms = re.search(r'(\d+)M', val, re.I)
      if ms: ctimes[1] = int(ms.group(1))
      ms = re.search(r'(\d+)D', val, re.I)
      if ms: ctimes[2] = int(ms.group(1))
      ms = re.search(r'(\d+)W', val, re.I)
      if ms: ctimes[2] += 7*int(ms.group(1))
      ms = re.search(r'(\d+)H', val, re.I)
      if ms: ctimes[3] = int(ms.group(1))
      ms = re.search(r'(\d+)N', val, re.I)
      if ms: ctimes[4] = int(ms.group(1))
      ms = re.search(r'(\d+)S', val, re.I)
      if ms: ctimes[5] = int(ms.group(1))
      for ctime in ctimes:
         if ctime > 0: return ctimes
      return self.pglog("{}: invalid '{}', must be (Y,M,W,D,H,N,S)".format(val, type), self.PGOPT['emlerr'])

   #  get group index from given option string
   def get_group_index(self, option, edate, ehour, freq):
      """Extract the dataset group index from a dsarch options string.

      Looks for ``-GI <index>`` (literal or pattern) or ``-GN <group_name>``
      in the options string.  Returns 0 if neither is found.

      Args:
         option (str): dsarch options string from dlupdt.options.
         edate (str): End data date for pattern replacement.
         ehour (int | None): End data hour.
         freq (list): Frequency array for pattern replacement.

      Returns:
         int: Group index, or 0 if not specified.
      """
      ms = re.search(r'-GI\s+(\S+)', option, re.I)
      if ms: return int(self.replace_pattern(ms.group(1), edate, ehour, freq))
      ms = re.search(r'-GN\s+(.*)$', option, re.I)
      if ms:
         grp = ms.group(1)
         if grp[0] == "'":
            grp = grp[1:]
            idx = grp.find("'")
            grp = grp[:idx]
         else:
            ms = re.match(r'^(\S+)', grp)
            if ms: grp = ms.group(1)
         pgrec = self.pgget("dsgroup", "gindex", "dsid = '{}' AND grpid = '{}'".format(self.params['DS'], self.replace_pattern(grp, edate, ehour, freq)), self.PGOPT['extlog'])
         if pgrec: return pgrec['gindex']
      return 0
