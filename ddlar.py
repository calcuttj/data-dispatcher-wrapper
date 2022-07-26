import subprocess
import argparse
import json

class DDLArInterface:
  def __init__(self, dataset, limit, namespace, lar_limit):
    self.dataset = dataset
    self.limit = limit
    self.namespace = namespace
    query_args = (self.dataset, self.namespace, self.limit)
    self.query = '''files from %s where 'namespace="%s"' limit %i'''%query_args
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
  def SetLarLimit(self, limit):
    self.lar_limit = limit
  def CreateProject(self):
    proc = subprocess.run('dd project create %s'%self.query,
                          shell=True, capture_output=True)
    self.proj_returncode = proc.returncode
    self.proj_id = proc.stdout.decode('utf-8').strip('\n')
    if self.proj_returncode == 0:
      self.proj_state = 'active'
  def Next(self):
    if self.proj_id < 0:
      raise ValueError('DDLArInterface::Next -- Project ID is %i. Has a project been created?'%self.proj_id)

    proc = subprocess.run('dd worker next -j -t %i %i'%(self.timeout, self.proj_id), shell=True, capture_output=True)
    self.next_output = proc.stdout.decode('utf-8')
    if self.next_output == 'timeout':
      self.hit_timeout = True
      return
    elif self.next_output == 'done':
      self.project_done = True
      return

    self.next_json = json.loads(proc.stdout.decode('utf-8'))
    self.next_name = self.next_json['name']
    self.next_replicas = self.next_json['replicas']
    print(proc.stdout)
  def PrintFiles(self):
    print('Printing files')
    for j in self.loaded_files:
      print(j['name'])
  def LoadFiles(self):
    count = 0
    while (count < self.lar_limit and not self.hit_timeout and
           self.proj_state == 'active'):
      self.Next()
      if len(self.next_replicas) > 0:
        self.loaded_files.append(self.next_json)
        count += 1
      else:
        print('Empty replicas -- marking as failed')
        proc = subprocess.run('dd worker failed -f %i %s:%s'%(self.proj_id, self.next_json['namespace'], self.next_json['name']),
                              shell=True, capture_output=True)
        ##Mark that file as failed -- TODO
    self.loaded = True
  def MarkFiles(self, failed=False):
    state = 'failed' if failed else 'done'
    for j in self.loaded_files:
      proc = subprocess.run('dd worker %s %i %s:%s'%(state, self.proj_id, j['namespace'], j['name']),
                            shell=True, capture_output=True)
      if proc.returncode == 0:
        print('Successfully marked %s as %s'%(j['name'], state))
      else:
        print('Error %s'%j['name'])

  def AttachProject(self, proj_id):
    self.proj_id = proj_id
    proc = subprocess.run('dd project show -j %i'%self.proj_id, shell=True,
                          capture_output=True)

    if proc.stdout.decode('utf-8') == 'null': self.proj_exists = False
    else:
      self.proj_exists = True
      self.proj_state = json.loads(proc.stdout.decode('utf-8'))['state']
  def RunLar(self, fcl, nevents, output=None):
    cmd = 'lar -c %s -s %s -n %i'%(fcl, self.lar_file_list, nevents)
    if output: cmd += ' -o %s'%output
    proc = subprocess.run(cmd, shell=True)
    self.lar_return = proc.returncode

    self.MarkFiles((self.lar_return != 0))

  def BuildFileListString(self):
    for j in self.loaded_files:
      if len(j['replicas']) > 0:
        #Get the first replica
        replica = j['replicas'][0]
        uri = replica['url']
        if 'https://eospublic.cern.ch/e' in uri: uri = uri.replace('https://eospublic.cern.ch/e', 'xroot://eospublic.cern.ch//e')
        self.lar_file_list += uri
        self.lar_file_list += ' '
      else:
        print('Empty replicas -- marking as failed')
        proc = subprocess.run('dd worker failed -f %i %s:%s'%(self.proj_id, self.next_json['namespace'], self.next_json['name']),
                              shell=True, capture_output=True)
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
