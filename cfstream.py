#!/usr/bin/env python

import sys
import os
import json
import cloudfiles

MAX_FILE_SIZE = 1000000
MAX_CHUNK_SIZE = 4096
CONTROL_FILE = '/tmp/mysqlbackup_control.sql'
VARIABLE_FILE = '/tmp/mysqlbackup_test.sql'
PREFIX = 'dumpfile'
CONFIG_FILE = '/Users/imsplitbit/.cfstream'
CONTAINER_NAME = 'backup_test'
DUMP_NAME = 'mysqldump.sql'

def filename(fileno):
    return '%s%08d.dat' % (PREFIX, fileno)

class StdInGen(object):
    def __init__(self):
        self.eof = False
        self.size = 0
        self.fileno = 1
        self.output_file = open(CONTROL_FILE, 'w')

    def generate(self, chunk_size = MAX_CHUNK_SIZE):
        self.eoc = False
        while True:
            if self.size >= (MAX_FILE_SIZE * self.fileno):
                self.eoc = True
                break
            data = sys.stdin.read(chunk_size)
            if len(data) == 0:
                self.eof = True
                break
            self.size += chunk_size
            self.output_file.write(data)
            yield data
        self.fileno += 1
    
    def close(self):
        self.output_file.close()

# read in our config json so that we don't checkin our username and api
# key into github.
print 'Loading config JSON\n'
with open(CONFIG_FILE) as fh:
    config = json.load(fh)

# open a connection to cloud files
print 'Opening an authenticated session with cloud files\n'
ch = cloudfiles.get_connection(str(config['username']), str(config['api_key']))

# create a custom container for the purposes of backing up
print 'Creating container backup_test\n'
bu_cont = ch.create_container(CONTAINER_NAME)

# Create our stdin output generator
sin_gen = StdInGen()

print 'Streaming backup...'
bu_file = filename(sin_gen.fileno)
print 'Creating backup file %s' % bu_file
bu_obj = bu_cont.create_object(bu_file)
while not sin_gen.eof:
    bu_obj.send(sin_gen.generate())
    if sin_gen.eoc:
        bu_file = filename(sin_gen.fileno)
        print 'Creating backup file %s' % bu_file
        bu_obj = bu_cont.create_object(bu_file)

# Close the control file
sin_gen.close()

# Create our manifest
manifest = bu_cont.create_object(DUMP_NAME)
manifest.write()
manifest.manifest = '%s/%s' % (bu_cont.name, PREFIX)
manifest.sync_manifest()

# for testing purposes get listings to prove everything is there
print '\nListing containers:'
conts = ch.get_all_containers()
for cont in conts:
    print '\t' + cont.name

print '\nListing objects in bu_cont:'
objs = bu_cont.list_objects()
objs.sort()
for obj in objs:
    print '\t' + obj

print '\nVerifying the backup is clean:'
with open(VARIABLE_FILE, 'w') as fh:
    obj = bu_cont.get_object(DUMP_NAME)
    for chunk in obj.stream():
        fh.write(chunk)

control = os.popen('md5 %s' % CONTROL_FILE)
control = control.readline().split()[3]

variable = os.popen('md5 %s' % VARIABLE_FILE)
variable = variable.readline().split()[3]

if control == variable:
    print "MATCHED"
else:
    print "BROKEN"

print '\nDeleting objects from %s:' % CONTAINER_NAME
for obj in objs:
    bu_cont.delete_object(obj)

print 'Deleting container %s' % CONTAINER_NAME
ch.delete_container(CONTAINER_NAME)
