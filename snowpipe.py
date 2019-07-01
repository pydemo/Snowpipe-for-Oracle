import os, sys, io, csv, time, boto, gzip, math
import pyodbc
e=sys.exit
from pprint import pprint as pp


try:
	from io import  BytesIO as cStringIO
except:
	try:
		import cStringIO
	except ImportError:
		import io as cStringIO	
	
	
	
	
def setKeytabCache(keyTabFile, keyTabPrincipal=''):
   if keyTabFile != '' and keyTabPrincipal != '':
    os.system("kinit -k -t {} {}".format(keyTabFile, keyTabPrincipal))
   else:
    message="keyTabFile {} or keyTabPrincipal not defined. Check environ variable KRB5_CLIENT_KTNAME".format(keyTabFile,keyTabPrincipal)
    print 'ERROR', message
    raise Exception(message)
			
encoding = 'utf-8'
write_file='_sqldata.csv'
stream =  io.BytesIO()
#line_as_list = [line.encode(encoding) for line in line_as_list]

	
			
dbenvars={ 'DB_SERVER':'DATAMART1', 'DEFAULT_DOMAIN':"CGROUP.COM"}


def get_cnt(cur,tab):
	cur.execute("SELECT count(1) from %s" % tab)
	return cur.fetchone()[0]

rid=0
file_rows=25000 #16384
s3_rows=10000
def s3_upload_rows( bucket, s3_key, data, suffix='.gz' ):

	rid=0
	
	assert data
	key = s3_key +suffix
	use_rr=False

	mpu = bucket.initiate_multipart_upload(key,reduced_redundancy=use_rr , metadata={'header':'test'})

	stream = cStringIO()
	
	compressor = gzip.GzipFile(fileobj=stream, mode='wb')

	uploaded=0
	
	#@timeit
	def uploadPart(partCount=[0]):
		global total_comp
		partCount[0] += 1
		stream.seek(0)
		mpu.upload_part_from_file(stream, partCount[0])
		total_comp +=stream.tell()
		
		stream.seek(0)
		stream.truncate()
	#@timeit
	def upload_to_s3():
		global total_size,total_comp, rid
		i=0
		
		while True:  # until EOF
			i+=1
			start_time = time.time()
			chunk=''
			#pp(data[0])
			tmp=[]
			if rid<len(data):
				tmp= data[rid:][:s3_rows]
				chunk=os.linesep.join(tmp)+os.linesep
			
			#print rid, len(chunk), len(data)
			rid +=len(tmp)
			if not chunk:  # EOF?
				compressor.close()
				uploadPart()
				mpu.complete_upload()
				break
			else:
				if sys.version_info[0] <3 and isinstance(chunk, unicode):
					compressor.write(chunk.encode('utf-8'))
				else:
					compressor.write(chunk)
				total_size +=len(chunk)
				if stream.tell() > 10<<20:  # min size for multipart upload is 5242880
					
					uploadPart()

	upload_to_s3()
	
	return key
def convertSize( size):
	if (size == 0):
		return '0B'
	size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
	i = int(math.floor(math.log(size,1024)))
	p = math.pow(1024,i)
	s = round(size/p,2)
	return '%s %s' % (s,size_name[i])

tbl='DY_FICCDISTRIBUTION'
stg='POSITION_MODEL_STAGE_TEST_2'
sch='ACCOUNTINGBI.POSITION'
wrh='LOAD_WH'
def bulk_copy(cur,  file_names):
	global tbl, stg, sch, LOAD_WH


	assert tbl and  stg and sch and wrh
	
	assert len(file_names)
	files="','".join(file_names)
	before=get_cnt(cur,tbl)
	start_time=time.time()

	if 1:
		
		cmd="""
COPY INTO 
%s 
FROM '@%s/%s/'
FILES=('%s')

		""" % (tbl,stg, 'DEMO', files)	

		

		if 1:
			cur.execute("USE WAREHOUSE %s" % wrh)
			cur.execute("USE SCHEMA %s" % sch)
			try:
				out=cur.execute(cmd) 
			except:
				print(cmd)
				raise
			pp(out)

			match=0
			for id, row in enumerate(cur.fetchall()):
				status, cnt = row[1:3]
				print('%s: Insert #%d, status: [%s], row count: [%s]' % ('DEMO', id, status, cnt))
				if status not in ['LOADED']:
					match +=1
			if match:
				raise Exception('Unexpected load status')
			cur.execute("commit")
			after=get_cnt(cur,tbl)
			print 'Rows inserted: ', after-before
			sec=round((time.time() - start_time),2)

				
if __name__=="__main__":
	if 1:
		bname= 'accounting-dev'
		AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
		AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
		conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
		bucket = conn.get_bucket(bname, validate=False)
					
	if 1:
		pyodbc.pooling = False
		tconn = pyodbc.connect("DSN=MART1;Database=TESTBI;WSID=ld-test;APP=MyTest;authenticator=https://home.okta.com;uid=s_dev_racct@home.com;pwd=***;autocommit=False;ROLE=s_dev_ROLE;")
		#tconn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
		tcur = tconn.cursor()
		tcur.execute("USE WAREHOUSE TEST_WH")
		tcur.execute("USE DATABASE ACTBI")
		tcur.execute("USE SCHEMA POSITION")

	if 1:
		
		keyTabFile=os.getenv('KRB5_CLIENT_KTNAME'); assert keyTabFile
		keyTabPrincipal=os.getenv('DATASTAGINGSQLUSER'); assert keyTabPrincipal
		#setKeytabCache(KEYTABFILE, os.getenv('DATASTAGINGSQLUSER'), None)
		print ("kinit -k -t {} {}".format(keyTabFile, keyTabPrincipal))
		e()
		os.system("kinit -k -t {} {}".format(keyTabFile, keyTabPrincipal))
		
	sconn = pyodbc.connect('Driver={Oracle in OraClient11g_home1};DBQ=pacfin;Uid=uid;Pwd=pw')	
	scur = sconn.cursor()
	scur.arraysize=file_rows
	#scur.setinputsizes([(pyodbc.SQL_WVARCHAR, 0, 0)])
	
	if 1:
		
		stmt="SELECT TransactionId,SettleDate,TransactionTypeCode,ClearedDate,CloseDate,CloseLeg, QuantityType,Quantity,AccountingDate,AsOfDateTime FROM %s" % tbl

		scur.execute(stmt)

		if 0:
			while True:
				rows = scur.fetchmany(file_rows)
				print len(rows)
				insert_data_2(rows)
				if not rows:
					break;
		if 0:
			with open(write_file, 'wb') as fh:
				writer = csv.writer(fh, dialect='excel', delimiter=',')
				while True:
					rows = scur.fetchmany(file_rows)
					print len(rows)
					writer.writerows(rows)
					if not rows:
						break;
		if 1:
			for col in scur.description:
				print col
			print 'Rows per file: ', file_rows
			print 'S3 chunk size:', s3_rows
			rows = scur.fetchmany(file_rows)
			fid=0
			files=[]
			total_all=comp_all=total_read=0
			while rows:
				total_size=total_comp=rid=0
				data=[]
				for row in rows:
					#out=[str(x) if isinstance(x, datetime.date) else x for x in list(row)]
					#data.append('^'.join(["'1971-01-01'" if x==None else str(x) if isinstance(x, int) else str(x) if x.isdigit() else "'%s'" % x for x in out]))
					out=[]
					for x in row:
						if x==None: out.append(b''); continue;
						if isinstance(x, datetime.date) or isinstance(x, datetime.datetime): out.append(str(x).encode('utf-8')); continue;
						if isinstance(x, int) or isinstance(x, float) : out.append(repr(x)); continue;
						if sys.version_info[0] <3:
							out.append(x) 
						else:
							out.append(x.encode())
					data.append('^'.join(out))
				timeFormat = "%Y-%m-%d_H-%M-%S"
				currentTimeStamp = time.strftime(timeFormat)

				
				if 1:
					start_time = time.time()
					fname='file_%d_%d.%s.DEMO.csv' % (fid, len(data), currentTimeStamp)
					s3_key_name='%s/%s/%s' % ('racct', 'DEMO', fname)
					key= s3_upload_rows(bucket,s3_key_name, data)
					total_read +=rid
					files.append(os.path.basename(key))
					
					sec=round((time.time() - start_time),2)
				rows = scur.fetchmany(file_rows)
				fid +=1
				total_all +=total_size
				comp_all +=total_comp
				
				print 'File: ', fid-1,', Rows: [%s],' %rid, 'Raw: ', convertSize(total_size), ', Compressed:', convertSize(total_comp)
			
			print 'Total raw: ', convertSize(total_all), 'Compressed:', convertSize(comp_all)
			print 'Total read:', total_read
			pp(files)
			bulk_copy(tcur, files)
