#! /usr/bin/env python
import cluster
import sys
import time
import os
import os.path
import sys
import argparse
import subprocess
import atexit

tail_proc = None

def exit_handler():
  if tail_proc is not None:
    tail_proc.kill()

def query_yes_no(question, default="yes"):
  """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is one of "yes" or "no".
    """
  valid = {"yes": True, "y": True, "ye": True,
           "no": False, "n": False}
  if default is None:
    prompt = " [y/n] "
  elif default == "yes":
    prompt = " [Y/n] "
  elif default == "no":
    prompt = " [y/N] "
  else:
    raise ValueError("invalid default answer: '%s'" % default)

  while True:
    sys.stdout.write(question + prompt)
    choice = raw_input().lower()
    if default is not None and choice == '':
      return valid[default]
    elif choice in valid:
      return valid[choice]
    else:
      sys.stdout.write("Please respond with 'yes' or 'no' "
                       "(or 'y' or 'n').\n")

if __name__ == '__main__':
  atexit.register(exit_handler)
  parser = argparse.ArgumentParser(
    description='Transparently run a job on Condor/Slurm/Local',
    fromfile_prefix_chars='@',
    add_help=False)
  parser.add_argument('--prefix', dest='prefix', type=str, required=False,
                      help='Direct output to PREFIX.[out/err/log] files')
  parser.add_argument('--outfile', dest='out', type=str, required=False,
                      help='Direct all output to single OUT file')
  parser.add_argument('--email', dest='address', type=str, required=False,
                      help='Send email to ADDRESS on job completion/error')
  parser.add_argument('--gpu', dest='gpu', action='store_true',
                      help='Request a gpu node')
  parser.add_argument('--follow', dest='follow', action='store_true',
                      help='Follow the job and print live output')
  condor_args = parser.add_argument_group('Condor specific arguments')
  condor_args.add_argument('--require', dest='req', type=str, required=False,
                           help='Specify condor requirement')
  condor_args.add_argument('--cpus', dest='cpus', type=int, required=False,
                           help='Request this many cpu cores')
  condor_args.add_argument('--disk', dest='disk', type=int, required=False,
                           help='Request this much disk space (in MB)')
  condor_args.add_argument('--memory', dest='mem', type=int, required=False,
                           help='Request this much memory (in MB)')
  condor_args.add_argument('--hold-after-evict', dest='hold_after_evict',
                           action='store_true', required=False,
                           help='Hold job (don\'t restart) if evicted')
  slurm_args = parser.add_argument_group('Slurm specific arguments')
  slurm_args.add_argument('--depend', dest='dep', metavar='PID', type=str,
                          required=False,
                          help='Only start after specified job completes')

  # Check for .clusterconf files in the currrent and home directory
  conf_files = []
  if os.path.isfile(os.path.join(os.path.expanduser('~'), '.clusterconf')):
    conf_files.append(os.path.join(os.path.expanduser('~'), '.clusterconf'))
  if os.path.isfile('.clusterconf'):
    conf_files.append('.clusterconf')
  # Insert conf_files immediately after argv[0] so these args are
  # added but don't override the args passed on command line
  for f in conf_files:
    sys.argv.insert(1, '@' + f)
  args, unknown = parser.parse_known_args()

  if len(unknown) < 1:
    parser.print_help()
    exit()

  j = cluster.get_env()
  j.set_executable(unknown[0])
  j.set_args(' '.join(unknown[1:]))
  j.set_gpu(args.gpu)
  j.set_email(args.address)
  if isinstance(j, cluster.CondorJob):
    if args.req:
      j.add_requirement(args.req)
    if args.cpus:
      j.request_cpus(args.cpus)
    if args.disk:
      j.request_disk(args.disk)
    if args.mem:
      j.request_memory(args.mem)
    if args.hold_after_evict:
      j.hold_after_evict()
  if isinstance(j, cluster.SlurmJob):
    if args.dep:
      j.set_depends(args.dep)

  if args.out:
    j.set_output(args.out)
  elif args.prefix:
    j.set_output_prefix(args.prefix)

  # Suppress just submits and returns
  if not args.follow:
    print j.submit()
    exit()
  else:
    if isinstance(j, cluster.CondorJob):
      print 'Executing on Condor: \"%s\"'%(' '.join(unknown))
    elif isinstance(j, cluster.SlurmJob):
      print 'Executing on Slurm: \"%s\"'%(' '.join(unknown))
    else:
      print 'Executing on Local Machine: \"%s\"'%(' '.join(unknown))

  # Ask to remove outfile if it already exists
  if os.path.isfile(j.output):
    query = j.output + ' already exists. Should I delete it?'
    if query_yes_no(query):
      os.remove(j.output)
    else:
      print 'Quitting'
      exit()

  pid = j.submit()
  if pid >= 0:
    sys.stdout.write('Waiting for job (PID '+str(pid)+') to start...')
    sys.stdout.flush()
    while not os.path.isfile(j.output):
      time.sleep(1)
    sys.stdout.write(' Done!\n')
    sys.stdout.write('Output: '+j.output+'\n')
    sys.stdout.flush()
    tail_proc = subprocess.Popen(['tail','-n','+0','-f',j.output])
    time.sleep(3)
    while j.alive():
      time.sleep(10)
