#! /usr/bin/env python
import cluster, argparse

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Remove jobs currently running'
                                   ' on the local cluster.')
  parser.add_argument('-a', dest='rmall', action='store_true',
                      help='Remove all jobs.')
  parser.add_argument('pids', type=float, nargs='*',
                      help='PIDs of jobs to remove.')
  args, unknown = parser.parse_known_args()
  if args.rmall:
    cluster.kill_all_jobs()
  elif args.pids:
    try: # Try to parse as int
      cluster.kill_jobs([int(i) for i in args.pids])
    except ValueError:
      cluster.kill_jobs(args.pids)
  else:
    parser.print_help()
