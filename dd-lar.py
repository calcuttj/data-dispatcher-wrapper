#!/usr/bin/env python

import subprocess
import argparse
parser = argparse.ArgumentParser(description = 'Wrapper around lar')

parser.add_argument('-c', type=str, help='Which fcl file to run',
                    default='eventdump.fcl')
parser.add_argument('--dataset', type=str, default='dc4:dc4')
parser.add_argument('--limit', type=int, default=10)
parser.add_argument('--namespace', type=str, default='dc4-hd-protodune')

#dc4:dc4 where 'namespace="dc4-hd-protodune"' limit 10
##-n 
##

args = parser.parse_args()

cmd = [
  'dd', 'project', 'create', 
]

query = '''files from %s where 'namespace="%s"' limit %i'''%(args.dataset, args.namespace, args.limit)
print(query)


#dd project create
subprocess.run(
  'metacat query %s'%query, shell=True)
exit()
