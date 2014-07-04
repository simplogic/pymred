# @file pymred.py
# @brief Haoop mapreduce framework for python. Main ideas comes from dumbo but more lightweight.
# If you don't have to working on a so limited environment like me, you should try dumbo.
# @author simplogic <simplogic@163.com>
# @version 0.0.1
# @date 2011-04-18

import sys, os, itertools, operator, glob, fileinput, subprocess, datetime

def usage(progname):
	print >> sys.stderr, "\n\
Usage %s\n\
Options:\n\
  -input          <path>    DFS input file(s) for the Map step\n\
  -output         <path>    DFS Output directory for the Reduce step\n\
  -inputfmt       Text<Default>|Code|SequenceText|SequenceCode\n\
  -outputfmt      Text<Default>|Code\n\
  -mode           hadoop<default>|local	Folowing options will be ignored if local mode is set\n\
  -hadoopexec     <hadoopexec>    Optional if environ variable \"HADOOPEXEC\" is set\n\
  -pythonexec     <pythonexec>    Optional if environ variable \"PYTHONEXEC\" is set\n\
  -streamingjar   <streamingjar>  Optional if environ variable \"STREAMINGJAR\" is set \n\
  -file           <file>    Optional  File/dir to be shipped in the Job jar file\n\
  -partitioner    JavaClassName  Optional.\n\
  -numReduceTasks <num>     Optional.\n\
  -inputreader    <spec>    Optional.\n\
  -cmdenv         <n>=<v>   Optional. Pass env.var to streaming commands\n\
  -mapdebug       <path>    Optional. To run this script when a map task fails \n\
  -reducedebug    <path>    Optional. To run this script when a reduce task fails\n\
  -verbose\n\
Generic options supported are\n\
  -conf <configuration file>     specify an application configuration file\n\
  -D <property=value>            use value for given property\n\
  -fs <local|namenode:port>      specify a namenode\n\
  -jt <local|jobtracker:port>    specify a job tracker\n\
  -files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster\n\
  -libjars <comma separated list of jars>   specify comma separated jar files to include in the classpath\n\
  -archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines\n\
	" % progname
	sys.exit(-1)

def incrcounter(group, counter, amount):
	print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, counter, amount)

def setstatus(message):
	print >> sys.stderr, 'reporter:status:%s' % message

def run(mapper, reducer, combiner = None):
	opts_ = parseargs(sys.argv)
	opts = getopts(opts_, [
		("mode", True, False, ["hadoop"]),
		("name", False, False, ["pymred"])])
	mode = opts["mode"][-1]
	name = opts["name"][-1]
	jobs = {"local": LocalJob(name, mapper, reducer, combiner, opts_), 
			"hadoop": HadoopJob(name, mapper, reducer, combiner, opts_)}
	if not jobs.has_key(mode):
		perr('unknown mode "%s"' % mode)
		return -1
	try:
		return jobs[mode].run()
	except OptionError, e:
		perr(str(e))
		usage(sys.argv[0])

class OptionError(Exception):
	def __init__(self, key, err):
		self.key = key
		self.err = err
	def __str__(self):
		return self.err

class Job:
	def __init__(self, name, mapper, reducer, combiner = None, opts = []):
		self.mapper = mapper
		self.reducer = reducer
		self.combiner = combiner
		self.opts = opts
		self.optkeys = ["input", "output", "inputfmt", "outputfmt", "file", "partitioner", 
				"numReduceTasks", "inputreader","cmdev", "mapdebug", "reducedebug", 
				"verbose", "conf", "D", "fs", "jt","files", "libjars", "archives",
				"hadoopexec", "pythonexec", "streamingjar"]
	def run(self, addopts = []):
		checkopts(self.opts + addopts, self.optkeys)
		return 0

class HadoopJob(Job):
	currid = 0
	def __init__(self, name, mapper, reducer, combiner = None, opts = []):
		Job.__init__(self, name, mapper, reducer, combiner, opts)
		self.jobid = 0 

	def run(self, addopts = []):
		self.jobid = HadoopJob.currid
		HadoopJob.currid = HadoopJob.currid + 1
		if iterating:
			return self.iterrun(iterfunc, inputfmt, outputfmt, jobid)
		retval = Job.run(self, addopts)
		if 0 != retval: 
			return retval
		opts_ = self.opts + addopts
		if os.environ.has_key("HADOOPEXEC"):
			opts_.insert(0, ("hadoopexec", os.environ["HADOOPEXEC"]))
		if os.environ.has_key("PYTHONEXEC"):
			opts_.insert(0, ("pythonexec", os.environ["PYTHONEXEC"]))
		if os.environ.has_key("STREAMINGJAR"):
			opts_.insert(0, ("streamingjar", os.environ["STREAMINGJAR"]))
		opts = getopts(opts_, [
			("input", False, True),
			("output", False, True),
			("inputfmt", True, False, ["Text"]),
			("outputfmt", True, False, ["Text"]),
			("hadoopexec", True, True),
			("pythonexec", True, True),
			("streamingjar", True, True)])
		if not os.path.exists(opts["hadoopexec"][-1]):
			perr('hadoop bin "%s" does not exist' % opts["hadoopexec"][-1])
			return -1
		if not os.path.exists(opts["streamingjar"][-1]):
			perr('streaming jar "%s" dost not exist' % opts["streamingjar"][-1])
			return -1
		prog = os.path.basename(sys.argv[0])
		global magicnum
		mapcmd = '%s %s %s iter map %s %s %d %s' % (opts["pythonexec"][-1], prog, magicnum,
				opts["inputfmt"][-1], opts["outputfmt"][-1], self.jobid, " ".join(sys.argv[1:]))
		redcmd = '%s %s %s iter reduce  %s %s %d %s' % (opts["pythonexec"][-1], prog, magicnum,
				opts["inputfmt"][-1], opts["outputfmt"][-1], self.jobid, " ".join(sys.argv[1:]))
		runcmd = '%s jar %s' % (opts["hadoopexec"][-1], opts["streamingjar"][-1])
		opts_.append(('file', os.path.abspath(sys.argv[0])))
		opts_.append(('file', os.path.abspath(__file__.rstrip("c").rstrip("o"))))
		opts_.append(('mapper', mapcmd))
		if self.reducer != None:
			opts_.append(('reducer', redcmd))
		if opts["inputfmt"][-1] == "SequenceText":
			opts_.append(('inputformat', 'SequenceFileAsTextInputFormat'))
		retval = execute(runcmd, opts_) 
		return retval

	def iterrun(self, itername, inputfmt, outputfmt, jobid):
		if self.jobid != int(jobid):
			return 0
		if itername == "map":	
			inputs = (line[:-1] for line in sys.stdin)
			loaditer = {"Text": loadtext(inputs), "Code": loadcode(inputs),
					"SequenceText": loadtext(inputs), "SequenceCode": loadcode(inputs)}
			if not loaditer.has_key(inputfmt):
				perr('unknown inputfmt "%s"' % inputfmt)
				sys.exist(-2)
			outputs = itermap(loaditer[inputfmt], self.mapper)
			if self.reducer != None:
				for output in dumpcode(outputs):
					print "\t".join(output)
			else:
				dumpiter = {"Text": dumptext(outputs), "Code": dumpcode(outputs)}
				if not dumpiter.has_key(outputfmt):
					perr('unknow outputfmt "%s"' % outputfmt)
					sys.exit(-3)
				for output in dumpiter[outputfmt]:
					print >>sys.stdout, "\t".join(output)
		elif itername == "reduce":
			inputs = loadcode(line[:-1] for line in sys.stdin)
			outputs=iterreduce(inputs, self.reducer)
			dumpiter = {"Text": dumptext(outputs), "Code": dumpcode(outputs)}
			if not dumpiter.has_key(outputfmt):
				perr('unknow outputfmt "%s"' % outputfmt)
				sys.exit(-3)
			for output in dumpiter[outputfmt]:
				print >>sys.stdout, "\t".join(output)
		else:
			perr('unknown itername "%s"' % itername)
			sys.exit(-1)
		sys.exit(0)

class LocalJob(Job):
	def __init__(self, name, mapper, reducer, combiner = None, opts = []):
		Job.__init__(self, name, mapper, reducer, combiner, opts)

	def run(self, addopts = []):
		retval = Job.run(self, addopts)
		if 0 != retval: 
			return retval
		opts = getopts(self.opts + addopts, [
			("input", False, True),
			("output", False, True),
			("inputfmt", True, False, ["Text"]),
			("outputfmt", True, False, ["Text"])])
		if not opts.has_key("input"):
			perr("input not set")
			return -1
		if not opts.has_key("output"):
			perr("output not set")
			return -1
		mapin = fileinput.FileInput(reduce(lambda x, y : x.extend(y) or x, 
			[glob.glob(p) for p in opts["input"]]))
		try:
			redout = open(opts["output"][-1], "w")
		except IOError, inst:
			perr("open output file \"%s\" failed" % opts["output"][-1])
			return -1
		inputs = (line[:-1] for line in mapin)
		loaditer = {"Text": loadtext(inputs), "Code": loadcode(inputs),
				"SequenceText": loadtext(inputs), "SequenceCode": loadcode(inputs)}
		inputfmt, outputfmt = opts["inputfmt"][-1], opts["outputfmt"][-1]
		if not loaditer.has_key(inputfmt):
			perr('unknow inputfmt "%s"' % inputfmt)
			return -2
		if self.reducer != None:
			outputs = itermapred(loaditer[inputfmt], self.mapper, self.reducer)
		else:
			outputs = sorted(itermap(loaditer[inputfmt], self.mapper))
		dumpiter = {"Text": dumptext(outputs), "Code": dumpcode(outputs)}
		if not dumpiter.has_key(outputfmt):
			perr('unknow outputfmt "%s"' % outputfmt)
			return -3
		for output in dumpiter[outputfmt]:
			print >> redout, "\t".join(output)
		return 0

def dumptext(outputs):																										
	newoutput = []
	for output in outputs:
		for item in output:
			if not item: 
				continue
			if not hasattr(item, '__iter__'):																				 
				newoutput.append(str(item))																				   
			else:
				newoutput.append('\t'.join(map(str, item)))																   
		yield newoutput
		del newoutput[:]																									  
																															  
def loadtext(inputs):																										 
	offset = 0
	for input in inputs:
		yield (offset, input)																								 
		offset += len(input)	

def mapfunc_iter(data, mapfunc):
	for (key, value) in data:
		for output in mapfunc(key, value):
			yield output

def itermap(data, mapfunc):
	try:
		return mapfunc(data)
	except TypeError:
		return mapfunc_iter(data, mapfunc)

def redfunc_iter(data, redfunc):
	for (key, values) in data:
		for output in redfunc(key, values):
			yield output

def iterreduce(data, redfunc):
	data = itertools.groupby(data, operator.itemgetter(0))
	data = ((key, (v[1] for v in values)) for key, values in data)
	try:
		return redfunc(data)
	except TypeError:
		return redfunc_iter(data, redfunc)

def itermapred(data, mapfunc, redfunc):
	return iterreduce(sorted(itermap(data, mapfunc)), redfunc)

def dumpcode(outputs):
	for output in outputs:
		yield map(repr, output)																							   
																															  
def loadcode(inputs):
	for input in inputs:																									  
		try:
			yield map(eval, input.split('\t', 1))																			 
		except (ValueError, TypeError):
			print >> sys.stderr, 'WARNING: skipping bad input (%s)' % input												   
			#if os.environ.has_key('dumbo_debug'):																			 
				#raise
			#incrcounter('Dumbo', 'Bad inputs', 1)																			 

def execute(cmd,
			opts=[],
			precmd='',
			printcmd=True,
			stdout=sys.stdout,
			stderr=sys.stderr):
	if precmd:
		cmd = ' '.join((precmd, cmd))
	args = ' '.join("-%s '%s'" % (key, value) for (key, value) in opts)
	if args:
		cmd = ' '.join((cmd, args))
	if printcmd:
		print >> stderr, 'EXEC:', cmd
	return system(cmd, stdout, stderr)

def system(cmd, stdout=sys.stdout, stderr=sys.stderr):
	if sys.version[:3] == '2.4':
		return os.system(cmd) / 256
	proc = subprocess.Popen(cmd, shell=True, stdout=stdout,
							stderr=stderr)
	return os.waitpid(proc.pid, 0)[1] / 256

def sorted(iterable, piecesize=None, key=None, reverse=False):
	if not piecesize:
		values = list(iterable)
		values.sort(key=key, reverse=reverse)
		for value in values:
			yield value
	else:  # piecewise sorted
		sequence = iter(iterable)
		while True:
			values = list(sequence.next() for i in xrange(piecesize))
			values.sort(key=key, reverse=reverse)
			for value in values:
				yield value
			if len(values) < piecesize:
				break

def parseargs(args):
	(opts, key, values) = ([], None, [])
	for arg in args:
		if arg[0] == '-' and len(arg) > 1:
			if key:
				opts.append((key, ' '.join(values)))
			(key, values) = (arg[1:], [])
		else:
			values.append(arg)
	if key:
		opts.append((key, ' '.join(values)))																				  
	return opts																											   
																															  
def getopts(opts, keys):																						 
	askedopts = {}
	for key in keys:
		k, delete, usedef, defval = key, False, True, []
		if type(key) == type(()):
			if len(key) > 0: k = key[0]
			if len(key) > 1: delete = key[1]
			if len(key) > 2: usedef = key[2]
			if len(key) > 3: defval = key[3]
		askedopts[k] = ([], delete, usedef, defval)
	(key, deleteidxs) = (None, [])																							
	for (index, (key, value)) in enumerate(opts):																			 
		key = key.lower()																									 
		if askedopts.has_key(key):																							
			askedopts[key][0].append(value)																					  
			if askedopts[key][1]:
				deleteidxs.append(index)
	for key in askedopts.keys():
			if len(askedopts[key][0]) != 0:
				askedopts[key] = askedopts[key][0]
			elif not askedopts[key][2]:
				askedopts[key] = askedopts[key][3]
			else:
				raise OptionError(key, 'option "%s" required' % key)
	for idx in reversed(deleteidxs):
		del opts[idx]
	return askedopts																							  
																															  
def getopt(opts, key):
	vals = getopts(opts, [key])
	if vals.keys.size() == 0:
		return None
	else:
		return vals[vals.keys[0]]

def checkopts(opts, keys):
	if len(filter(lambda x: x[0] not in keys, opts)) != 0:
		for k, v in opts:
			if k not in keys:
				raise OptionError(k, '"%s" is not a valid option' % k)

def perr(errstr):
	print >> sys.stderr, "Error: [%s] %s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), errstr)

magicnum = "001209693"
iterating = False
if len(sys.argv) >= 7 and sys.argv[1] == magicnum and sys.argv[2] == "iter":
	iterating = True
	iterfunc = sys.argv[3]
	inputfmt = sys.argv[4]
	outputfmt = sys.argv[5]
	jobid = sys.argv[6]
	for i in range(6, 0, -1):
		del sys.argv[i]

