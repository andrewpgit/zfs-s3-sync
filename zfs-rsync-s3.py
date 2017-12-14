 #!/use/bin/env python3



from multiprocessing import JoinableQueue, Process  
from subprocess import call, Popen, PIPE, STDOUT
import sys,time, os, logging
import boto3
from io import BytesIO

INFO_MEDIA = {'mc':('/mnt/mc-media/','/data/mc-media'),
		   	  'pr':['/mnt/pr-media/packagist','/data/pr-media'],
		      'mm':['/mnt/mm-media/media','/data/mm-media']}

#INFO_MEDIA = {'mc':['/etc/sysconfig','/test/andrew'],
#			  'local':['/home','/test/andrew'] }


BUCKET_NAME = 'bucket name'
CUSTOMKEY   = 'custom key'
BUFFER_SIZE = 52428800
FILENAME = '/var/log/backup-snap-s3.log'

logging.basicConfig(filename=FILENAME,level=logging.INFO,
					format='[%(asctime)s] - [%(levelname)s] - %(message)s')

class MediaSync(object):

	
	def __init__(self,media,verbose=False):

		self.media = media
		self.verbose = verbose
		self.dir_from, self.dataset = INFO_MEDIA[self.media]
		self.filename = os.path.dirname(__file__)
	
	def sync(self):
		"""Creates rsync command"""
		if self.media == 'mc':
			exclude  = "--exclude-from '%s/mc_exclude.txt'" % self.filename if self.filename  else "--exclude-from 'mc_exclude.txt'"
			prg = self.command(cmd="rsync -av --delete --stats %s %s %s/" % (exclude,self.dir_from,self.dataset),
				  			  bufsize = -1,
				  			  stdout=open('/tmp/%s.logs' % (self.media), 'a'),
				  			  stderr=STDOUT)
			return prg
						
		elif self.media == 'pr':
			include = " --prune-empty-dirs --include '*/' --include '*.zip' --include '*.tgz' "
			exclude = " --exclude '*'"
			prg = self.command(cmd="rsync -av --stats --delete %s %s %s %s/" % (include,exclude,self.dir_from,self.dataset),
							   bufsize = -1,
							   stdout=open('/tmp/%s.logs' % (self.media), 'a'),
							   stderr=STDOUT)
			return prg			
	
		elif self.media == 'mm':
 			exclude = "--exclude-from '%s/mm_exclude.txt'" % self.filename if self.filename else "--exclude-from 'mm_exclude.txt'"
 			prg = self.command(cmd="rsync -av --stats --delete %s %s %s/ " % (exclude,self.dir_from,self.dataset),
 							   bufsize = -1,
 							   stdout=open('/tmp/%s.logs' % (self.media), 'a'),
 							   stderr=STDOUT)
 			return prg
		else:
			prg = self.command(cmd="rsync -av --stats --delete %s %s"   % (self.dir_from, self.dataset),
							   bufsize = -1,
							   stdout=open('/tmp/%s.logs' % (self.media), 'a'),
							   stderr=STDOUT)
			return prg
	
	def create_snap(self):
		dtime = time.strftime("%d%m%Y-%R")
		dataset = self.dataset.strip("/")
		snapname = "%s@%s" % (dataset,dtime)
	
		sp = self.command(cmd="zfs snapshot %s" % (snapname))
		return snapname
		 
	def list_snap(self, command):
		dataset = self.dataset.strip("/")
		sp = self.command(cmd="zfs list -r -H -t snap -o name -s creation  %s | %s" %  (dataset,command))
		return sp
	

	def increm_snap(self, args_snap):
		"""Create full snapshot or incremental"""

		if len(args_snap) == 0:
			logging.warning("Exit")
			sys.exit()
		if len(args_snap) == 1:
			logging.info("Full snapshot created : %s " % (args_snap.get('last')))
			sp1 = self.command(cmd="zfs send %s" % (args_snap.get('last')))
			#sp2 = self.command(cmd="gzip -fc",stdin=sp1.stdout)
			return sp1
		else:
			logging.info("Create Incremental snapshot: %s  %s" % (args_snap.get('previous'), args_snap.get('last')))
			sp1 = self.command(cmd="zfs send -i  %s  %s" % (args_snap.get('previous'), args_snap.get('last')))
			sp2 = self.command(cmd="gzip -fc",stdin=sp1.stdout)
			return sp2

	def remove_snap_all(self):
		dataset = "%s@%%" % self.dataset.strip("/")
		sp = self.command(cmd="zfs destroy -rv  %s" % (dataset))
		return sp
	
	def remove_part_snap(self, snapname):
		"""Remove first 30 snapshot"""
		sp = self.command(cmd="zfs destroy %s" % (snapname))
		return sp

	def command(self,cmd,**new_args):
		
		args = dict({'bufsize': -1 ,'shell': True,'stdin': None, 'stdout': PIPE,'stderr' : PIPE}, **new_args)

		
		runcom = Popen(cmd, **args)
		if self.verbose:
			logging.info(cmd)
			runcom.wait()
			if runcom.returncode != 0:
				raise Exception("Command fail:", { "Error": runcom.stderr.read() })
		return runcom 	




s3 = boto3.client('s3')


def s3get_upload_id(bucket_name,key_name): 
	mu = s3.create_multipart_upload(ACL="private",
									Bucket=bucket_name,
									Key=key_name,
									SSECustomerKey=CUSTOMKEY, 
									SSECustomerAlgorithm='AES256')
	upload_id  = mu['UploadId']
	return upload_id


def s3complete_mu(bucket_name,key_name,upload_id,parts):

	complete = s3.complete_multipart_upload(
					Bucket=BUCKET_NAME,
					Key=key_name,
					UploadId=upload_id,
					MultipartUpload={"Parts": parts})
	return complete

def snap_count(media):
	"""list of count of snapshots"""
	p = MediaSync(media,verbose=True)
	out = p.list_snap(command='wc -l')
	count = out.stdout.read().decode('utf-8').strip("\n")
	return int(count)


def prc_sync(number,input_q,output_q):
	"""multiprocessing rsync"""
	while True:
		item = input_q.get()
		logging.info("Process rsync run: %s => %s" % (number,item)) 
		p = MediaSync(item)
		p_run = p.sync()
		p_run.wait()
		if p_run.returncode == 0:
			logging.info("Rsync process was ended successfully: %s" % (item))
			output_q.put(item)
			input_q.task_done()
		else:
			logging.error("Rsync process was ended with error: %s" % (item))

def prc_snap(number,output_q):
	"""multiprocessing snapshots and snap uploads to aws s3 bucket """
	while True:
		
		item = output_q.get()
		logging.info("Process snapshot number: %s %s" % (number,item))
		
		snapdict = {}
		dateday = time.strftime("%d")
		key_name = "%s-media/%s-snaphost_%s.gz" % (item,item,time.strftime("%d%m%Y-%H:%M"))
		upload_id = s3get_upload_id(BUCKET_NAME, key_name)
		logging.info("UploadId : %s" % (upload_id))
		
		p = MediaSync(item)
		lastsp = p.list_snap(command='tail -1')
		out = lastsp.stdout.read().decode('utf-8').strip("\n")

		pc = MediaSync(item,verbose=True)
		if dateday == '01':
			"""if date of day 01, it will be created full backup"""
			snapdict['last'] = pc.create_snap()
		elif out == "":
			"""if previous snapshot is empty than will be created full backup"""
			snapdict['last'] = pc.create_snap()
		else:
			"""incremental backup between snapshots are created"""
			snapdict['previous'] = out
			snapdict['last'] = pc.create_snap()

		if snap_count(item) > 60:
			logging.info("Delete first 30 snapshot for dataset:[ %s ]" % (item))
			list_snapshot = pc.list_snap(command='head -30')
			for line in list_snapshot.stdout.read().decode('utf-8').splitlines():
				remove_part_snap(line)
			logging.info("Delete old snapshot was done for %s" % (item))

		"""data sync to aws s3 bucket"""
		print("BUFFER_SIZE : %d" % (BUFFER_SIZE))
		
		incr = p.increm_snap(snapdict)
		data = incr.stdout.read(BUFFER_SIZE)
		part_number = 1
		parts = []
		size = 0
		
		while data:	
			fd = BytesIO(data)
			upload_data = s3.upload_part(Body=fd,
									Bucket=BUCKET_NAME,
                                    Key=key_name,
                                    UploadId=upload_id,
                                    SSECustomerKey=CUSTOMKEY,
                                    SSECustomerAlgorithm='AES256',
                                    PartNumber=part_number)
			size += len(data)
			#print(upload_data["ETag"], "=>", item, "Size:", size,"bytes")

			parts.append({"PartNumber": part_number, "ETag": upload_data["ETag"]})
			fd.seek(0)
			data = incr.stdout.read(BUFFER_SIZE)
			part_number += 1

		result = s3complete_mu(BUCKET_NAME, key_name, upload_id, parts)
		logging.info("Bucket: %s  -> Snapshot name: %s  Size: %d bytes" % (result['Bucket'], result['Key'], size))
		output_q.task_done()



def supplier(input_q,sequence):
	for key in sequence:
		input_q.put(key)

if __name__=="__main__":
	"""Run main process jobs"""
	in_q = JoinableQueue()
	out_q =JoinableQueue()

	jobs1 = []
	for i in range(0,2):
		p1 = Process(target=prc_sync, args=[i,in_q,out_q])
		jobs1.append(p1)
		p1.daemon = True
		p1.start()

	jobs2 = []
	for i in range(0,1):
		p2 = Process(target=prc_snap, args=[i,out_q])
		jobs2.append(p2)
		p2.daemon = True
		p2.start()

	supplier(in_q,INFO_MEDIA)
	logging.info('Main process join in queue')
	in_q.join()
	out_q.join()
	logging.info('Done')


