#! /usr/bin/env python
import cluster, argparse

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='List jobs currently running'
                                   ' on the local cluster.')
  parser.add_argument('-a', dest='lsall', action='store_true',
                      help='List all jobs (not only yours).')
  args, unknown = parser.parse_known_args()

  if args.lsall:
    cluster.list_all_jobs()
  else:
    cluster.list_jobs()
