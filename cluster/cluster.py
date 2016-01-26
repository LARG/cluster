import tempfile, subprocess, getpass, os, signal, socket, psutil

def get_env():
  '''Determines the type of job for the current environment.

  Supported environments are Condor/Slurm/Local.

  '''
  if socket.getfqdn().endswith('tacc.utexas.edu'):
    return SlurmJob()
  if socket.getfqdn().endswith('cs.utexas.edu'):
    output = subprocess.Popen(['condor_config_val', 'DAEMON_LIST'],
                              stdout=subprocess.PIPE).communicate()[0]
    if 'SCHEDD' in output:
      return CondorJob()
  return Job()

def list_jobs():
  '''Lists the jobs currently executing.'''
  get_env().list_jobs()

def list_all_jobs():
  '''Lists all the jobs current executing (not only your own).'''
  get_env().list_all_jobs()

def kill_all_jobs():
  '''Kills all the currently executing jobs.'''
  get_env().kill_all_jobs()

def kill_jobs(pid_list):
  '''Kills only the jobs in the provided pid list.'''
  get_env().kill_jobs(pid_list)

class Job:
  '''A locally running job.'''
  def __init__(self, executable='', args=''):
    self.executable       = executable
    self.output           = 'job.out'
    self.error            = 'job.err'
    self.log              = 'job.log'
    self.arguments        = args
    self.use_gpu          = False
    self.pid              = None
    self.proc             = None
    self.username         = getpass.getuser()
    self.completion_email = None

  def set_executable(self, executable):
    '''Specify the executable to be run.'''
    self.executable = executable

  def set_args(self, args):
    '''Specify the arguments for the job.'''
    self.arguments = args

  def set_gpu(self, use_gpu):
    '''Toggle gpu usage. Applicable to Condor/Slurm jobs.'''
    self.use_gpu = use_gpu

  def set_output(self, out):
    '''Combine all output/error/log into the provided file.'''
    self.output = out
    self.error  = out
    self.log    = out

  def set_output_prefix(self, prefix):
    '''Set a common prefix for the job's output/error/log files.'''
    self.error  = prefix + '.err'
    self.output = prefix + '.out'
    self.log    = prefix + '.log'

  def set_email(self, email):
    '''If set, send an email on completion of job.'''
    self.completion_email = email

  def submit(self):
    '''Run the job.'''
    cmd = self.executable + ' ' + self.arguments
    if self.completion_email:
      subject = '[Complete] - ' + self.executable
      cmd = '(' + cmd + '; echo ' + cmd + ' | mail -s \"' + \
             subject + '\" ' + self.completion_email + ') '
    proc = subprocess.Popen(cmd, stdout=open(self.output,'w'),
                            stderr=open(self.error,'w'),
                            shell=True, preexec_fn=os.setsid)
    parent_pid = proc.pid
    self.pid = parent_pid
    try:
      p = psutil.Process(parent_pid)
      children = p.get_children(recursive=True)
      if len(children) > 0:
        self.pid = children[0].pid
    except:
      print 'Unable to determine pid of child process. Guessing pid=parent+1.'
      self.pid = self.pid + 1
    return self.pid

  def alive(self):
    '''Checks if the job is alive.'''
    try:
      os.kill(self.pid, 0)
    except OSError:
      return False
    return True

  def kill(self):
    '''Kills the job.'''
    self.kill_jobs([self.pid])

  def list_jobs(self):
    '''Lists your jobs running in the current environment.'''
    subprocess.Popen(['ps','-u',self.username]).wait()

  def list_all_jobs(self):
    '''Lists all jobs running in the current environment.'''
    subprocess.Popen(['ps','-ef']).wait()

  def kill_all_jobs(self):
    '''Kills all jobs in the current environment.'''
    print 'You don\'t want me to do this. Use kill_jobs() instead.'

  def kill_jobs(self, pid_list):
    '''Kills only the jobs in the provided pid list.'''
    import signal
    for pid in pid_list:
      os.kill(pid, signal.SIGTERM)

class CondorJob(Job):
  '''A job to be executed on Condor.'''
  def __init__(self, executable='', args=''):
    Job.__init__(self, executable, args)
    self.infile       = None
    self.group        = 'GRAD'
    self.project      = 'AI_ROBOTICS'
    self.description  = 'Research'
    self.requirements = None
    self.universe     = None
    self.getenv       = True
    self.cpus         = None
    self.disk         = None
    self.memory       = None

  def submit(self):
    '''Submit the job to Condor.'''
    f = open('condor_submit','w') #tempfile.NamedTemporaryFile()
    f.write('+Group = \"'+self.group+'\"\n')
    f.write('+Project = \"'+self.project+'\"\n')
    f.write('+ProjectDescription = \"'+self.description+'\"\n')
    if self.universe:
      f.write('universe = '+self.universe+'\n')
    if self.getenv:
      f.write('getenv = true\n')
    f.write('Executable = '+self.executable+'\n')
    if self.arguments:
      f.write('Arguments = '+self.arguments+'\n')
    if self.requirements:
      f.write('Requirements = '+self.requirements+'\n')
    if self.infile:
      f.write('Input = '+self.infile+'\n')
    f.write('Error = '+self.error+'\n')
    f.write('Output = '+self.output+'\n')
    f.write('Log = '+self.log+'\n')
    if self.cpus:
      f.write('request_cpus = '+str(self.cpus)+'\n')
    if self.disk:
      f.write('request_disk = '+str(self.disk)+'\n')
    if self.memory:
      f.write('request_memory = '+str(self.memory)+'\n')
    if self.completion_email:
      f.write('Notify_User = '+str(self.completion_email)+'\n')
      f.write('Notification = Always\n')
    if self.use_gpu:
      f.write('+GPUJob = true\n')
    f.write('Queue \n')
    f.flush()
    condorFile = f.name
    output = subprocess.Popen(["condor_submit","-verbose",condorFile],
                              stdout=subprocess.PIPE).communicate()[0]
    f.close()
    s = output.find('** Proc ')+8
    procID = output[s:output.find(':\n',s)]
    try:
      self.pid = float(procID)
    except ValueError:
      print output
      self.pid = None
    return self.pid

  def set_gpu(self, use_gpu):
    self.use_gpu = use_gpu
    if use_gpu == True:
      self.add_requirement('TARGET.GPUSlot')

  def add_requirement(self, requirement):
    '''Add a requirement to the job.'''
    if not self.requirements:
      self.requirements = requirement
    else:
      self.requirements += ' && ' + requirement

  def hold_after_evict(self):
    '''Add a requirement that puts the job on hold if it is evicted.'''
    self.add_requirement('NumJobStarts == 0')

  def request_cpus(self, requested_cpus):
    ''' Request a certain number of cpu cores.'''
    self.cpus = requested_cpus

  def request_disk(self, requested_disk):
    ''' Request a certain amount of disk space (in MB).'''
    self.disk = str(requested_disk) + 'M'

  def request_memory(self, requested_memory):
    ''' Request a certain amount of memory (in MB).'''
    self.memory = requested_memory

  def alive(self):
    output = subprocess.Popen(['condor_q', str(self.pid)],
                              stdout=subprocess.PIPE).communicate()[0]
    return str(self.pid) in output

  def list_jobs(self):
    print subprocess.Popen(['condor_q', '-wide', self.username],
                           stdout=subprocess.PIPE).communicate()[0]

  def list_all_jobs(self):
    print subprocess.Popen(['condor_q', '-wide'],
                           stdout=subprocess.PIPE).communicate()[0]

  def kill_all_jobs(self):
    print subprocess.Popen(['condor_rm', self.username],
                           stdout=subprocess.PIPE).communicate()[0]

  def kill_jobs(self, pid_list):
    cmd = ['condor_rm']
    for pid in pid_list:
      pid_str = str(pid)
      if type(pid) == int:
        pid_str += '.0'
      cmd.append(pid_str)
    print subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]

class SlurmJob(Job):
  '''A job to be executed on Slurm.'''
  def __init__(self, executable='', args=''):
    Job.__init__(self, executable, args)
    self.hours = 12
    self.minutes = 0
    self.queue = 'gpu'
    self.dep = None

  def set_job_time(self, hours, minutes):
    '''Specify the expected job runtime in hours and minutes.'''
    assert(minutes < 60 and minutes >= 0)
    assert(hours >= 0)
    self.hours = hours
    self.minutes = minutes

  def set_depends(self, pid):
    '''Specify the pid that this job depends on. This job will not execute
    until the specified pid finishes.'''
    self.dep = pid

  def set_queue(self, queue):
    '''Specify the queue that the job should enter.'''
    self.queue = queue

  def set_gpu(self, use_gpu):
    if use_gpu:
      self.set_queue('gpu')

  def submit(self):
    '''Submit the job to Slurm.'''
    f = tempfile.NamedTemporaryFile()
    f.write('#!/bin/bash\n')
    f.write('#SBATCH -J '+str(self.executable)+'\n')
    f.write('#SBATCH -o '+self.output+'\n')
    f.write('#SBATCH -e '+self.error+'\n')
    f.write('#SBATCH -p '+self.queue+'\n')
    f.write('#SBATCH -N 1\n')
    f.write('#SBATCH -n 20\n')
    f.write('#SBATCH -t '+str(self.hours)+':'+str(self.minutes)+':00\n')
    if self.dep:
      f.write('#SBATCH -d '+self.dep+'\n')
    if self.completion_email:
      f.write('#SBATCH --mail-type=end\n')
      f.write('#SBATCH --mail-user='+self.completion_email+'\n')
    f.write(self.executable+' '+self.arguments+'\n')
    f.flush()
    jobFile = f.name
    output = subprocess.Popen(["sbatch",jobFile],
                              stdout=subprocess.PIPE).communicate()[0]
    f.close()
    start = output.find('Submitted batch job ')+len('Submitted batch job ')
    procID = output[start:output.find('\n',start)]
    try:
      self.pid = int(procID)
    except ValueError:
      print output
      self.pid = None
    return self.pid

  def alive(self):
    output = subprocess.Popen(['squeue','-j',str(self.pid)],
                              stdout=subprocess.PIPE).communicate()[0]
    return str(self.pid) in output

  def list_jobs(self):
    print subprocess.Popen(['squeue','-u',self.username],
                           stdout=subprocess.PIPE).communicate()[0]

  def list_all_jobs(self):
    print subprocess.Popen(['squeue','-l'],
                           stdout=subprocess.PIPE).communicate()[0]

  def kill_all_jobs(self):
    print subprocess.Popen(['scancel','-u',self.username],
                           stdout=subprocess.PIPE).communicate()[0]

  def kill_jobs(self, pid_list):
    cmd = ['scancel'] + [str(pid) for pid in pid_list]
    print subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
