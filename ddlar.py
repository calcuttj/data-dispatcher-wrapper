import subprocess
import argparse
import json

from data_dispatcher.api import DataDispatcherClient
dd_client = DataDispatcherClient(server_url='https://metacat.fnal.gov:9443/dune/dd/data',
                              auth_server_url='https://metacat.fnal.gov:8143/auth/dune')
from metacat.webapi import MetaCatClient
mc_client = MetaCatClient()

class DDLArInterface:
  def __init__(self, dataset, limit, namespace, lar_limit):
    self.dataset = dataset
    self.limit = limit
    self.namespace = namespace
    query_args = (self.dataset, self.namespace, self.limit)
    self.query = '''files from %s where namespace="%s" limit %i'''%query_args
    self.lar_limit = lar_limit
    self.proj_id = -1
    self.proj_exists = False
    self.proj_state = None
    self.loaded_files = []
    self.loaded_file_uris = []
    self.loaded = False
    self.timeout = 60
    self.hit_timeout = False
    self.lar_return = -1
    self.lar_file_list = ''
    self.next_failed = False
    self.next_replicas = []
    self.next_name = ''

  def SetLarLimit(self, limit):
    self.lar_limit = limit
  def CreateProject(self):
    query_files = mc_client.query(self.query)
    proj_dict = dd_client.create_project(query_files, query=self.query)
    self.proj_state = proj_dict['state']
    self.proj_id = proj_dict['project_id']
    self.proj_exists = True
    print(proj_dict)

  def Next(self):
    if self.proj_id < 0:
      raise ValueError('DDLArInterface::Next -- Project ID is %i. Has a project been created?'%self.proj_id)
    ## exists, state, etc. -- TODO
    self.next_output = dd_client.next_file(self.proj_id)['handle']

    if self.next_output == None:
      self.next_failed = True
      return

    self.next_name = self.next_output['name']
    self.next_replicas = list(self.next_output['replicas'].values())

  def PrintFiles(self):
    print('Printing files')
    for j in self.loaded_files:
      print(j['name'])
  def LoadFiles(self):
    count = 0
    while (count < self.lar_limit and not self.next_failed and
           self.proj_state == 'active'):
      print('Attempting fetch %i/%i'%(count, self.lar_limit), self.next_failed)
      self.Next()
      if self.next_output == None:
        continue
      elif len(self.next_replicas) > 0:
        self.loaded_files.append(self.next_output)
        count += 1
      else:
        print('Empty replicas -- marking as failed')
        dd_client.file_failed(self.proj_id, '%s:%s'%(self.next_output['namespace'], self.next_output['name']))
        ##Mark that file as failed -- TODO
    self.loaded = True
  def MarkFiles(self, failed=False):
    state = 'failed' if failed else 'done'
    for j in self.loaded_files:
      if failed:
        dd_client.file_failed(self.proj_id, '%s:%s'%(j['namespace'], j['name']))
      else:
        dd_client.file_done(self.proj_id, '%s:%s'%(j['namespace'], j['name']))

  def AttachProject(self, proj_id):
    self.proj_id = proj_id
    proj = dd_client.get_project(proj_id)
    if proj == None:
      self.proj_exists = False
    else:
      self.proj_exists = True
      self.proj_state = proj['state']

  def RunLar(self, fcl, nevents, output=None):
    cmd = 'lar -c %s -s %s -n %i'%(fcl, self.lar_file_list, nevents)
    if output: cmd += ' -o %s'%output
    proc = subprocess.run(cmd, shell=True)
    self.lar_return = proc.returncode

    self.MarkFiles((self.lar_return != 0))

  def BuildFileListString(self):
    for j in self.loaded_files:
      replicas = list(j['replicas'].values())
      if len(replicas) > 0:
        #Get the first replica
        replica = replicas[0]
        uri = replica['url']
        if 'https://eospublic.cern.ch/e' in uri: uri = uri.replace('https://eospublic.cern.ch/e', 'xroot://eospublic.cern.ch//e')
        self.lar_file_list += uri
        self.lar_file_list += ' '
      else:
        print('Empty replicas -- marking as failed')
        
        ##TODO -- pop entry
        dd_client.file_failed(self.proj_id, '%s:%s'%(j['namespace'], j['name']))
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'Wrapper around lar')
  
  parser.add_argument('-c', type=str, help='Which fcl file to run',
                      default='eventdump.fcl')
  parser.add_argument('--dataset', type=str, default='dc4:dc4')
  parser.add_argument('--limit', type=int, default=10)
  parser.add_argument('--namespace', type=str, default='dc4-hd-protodune')
  parser.add_arugment('--lar-limit', type=int, default=1)
  args = parser.parse_args()
  
  #Build Query
  query = '''files from %s where 'namespace="%s"' limit %i'''%(args.dataset, args.namespace, args.limit)
  print(query)
  
  #Create Project
  proc = subprocess.run('dd project create %s'%query, shell=True, capture_output=True)
  print(proc.returncode)
  
  #Get Project ID
  proj_id = int(proc.stdout.decode('utf-8').strip('\n'))
  print(proj_id)
  
  count = 0
  files_to_process = []
  hit_timeout
  while count < args.lar_limit and hit_timeout == False:
    proc = subprocess.run('dd worker next %i'%proj_id, shell=True, capture_output=True)
    file_json = proc.stdout.decode('utf-8')
    print(proc.returncode)
    print(file_did)
  
  
    proc = subprocess.run('dd worker done %i %s'%(proj_id, file_did), shell=True, capture_output=True)
    print(proc.returncode)
    print(proc.stdout.decode('utf-8'))
  
    count += 1
  
  
  #subprocess.run('dd project cancel %i'%proj_id, shell=True)
  exit()
