#!/usr/bin/python3
import multiprocessing
from multiprocessing.managers import BaseManager
import os
import time
import datetime
import socket
import subprocess
import sys
import re
import pathlib

DEFAULT_AUTHKEY=b'x'
DEFAULT_PORT=11111
DEFAULT_PPB=os.cpu_count()  #Processes Per Blockrunner
DEFAULT_BID=socket.gethostname()  #Blockrunner ID
DEFAULT_TIMEOUT=0  #no timeout
DEFAULT_OUTPUT="stat" #or collect or null
DEFAULT_RUNNEROUTPUT="file" #or null
DEFAULT_RUNNERDIR= "/tmp/commandsrun"

class CommandDispatcher():
    def __init__(self, port=DEFAULT_PORT,authkey=DEFAULT_AUTHKEY,timeout=DEFAULT_TIMEOUT,output=DEFAULT_OUTPUT):
        self.port = port
        self.timeout = timeout
        self.output=output
        self.authkey=authkey
        self.job_q = multiprocessing.SimpleQueue()
        self.result_q = multiprocessing.SimpleQueue()
        self._manager = self._create_manager()
        self.commands=[]

    def _create_manager(self):
        class JobQueueManager(BaseManager):
            pass
        JobQueueManager.register('get_job_q', callable=lambda: self.job_q)
        JobQueueManager.register('get_result_q', callable=lambda: self.result_q)
        return (JobQueueManager(address=('', self.port), authkey=self.authkey))

    @classmethod
    def _keyval_command_validity(cls,key,val):
        if key == "commandid":
            return 0
        elif key == "timeout":
            try:
                float(val)
            except ValueError:
                return 1
            return 0
        else:
            return -1

    def load_commands(self,files):
        self.commads=[]
        st_blank=r'\s*'
        st_keyval=r'(?P<key>[-\w_\.]+)=(?P<val>[-\w_\.]+)'
        st_keyvals="(%s\s+)*" % (st_keyval,)
        st_command=r'([^=\s]+)(\s.*)?'
        st_line="^(?P<keyvals>%s)(?P<command>%s)$" % (st_keyvals,st_command)
        re_blank=re.compile(st_blank)
        re_line=re.compile(st_line)
        re_keyval=re.compile(st_keyval)

        seqid=0
        for file in files:
            with open(file,mode='r') as f:
                for n,line in enumerate(f,1):
                    if re_blank.fullmatch(line):
                        continue  #if only spaces... just skip it
                    m=re_line.match(line)
                    if (m is not None):
                        command=m.group('command')
                        keyvals=m.group('keyvals')
                        kv=dict(re_keyval.findall(keyvals))
                        for k,v in kv.items():
                            vflag=self._keyval_command_validity(k,v)
                            if vflag < 0:
                                raise(Exception("unknown key '%s' in line %i in file %s: %s" % (k,n,file,line)))
                            elif vflag > 0:
                                raise (Exception("not valid value '%s' for key '%s' in line %i in file %s: %s" % (v,k, n, file, line)))
                        job={}
                        job['commandid']=kv.get('commandid',str(seqid))  #if exists command key, it has precedence, otherwise a sequence number
                        to=float(kv.get('timeout',self.timeout))  #if exists command key, it has precedence.
                        if (to == 0):
                            job['timeout']=None
                        else:
                            job['timeout']=to
                        job['tag']="cmd"  #  this is a commmand, baby
                        if job['timeout'] is not None:
                            job['timeout']=float(job['timeout'])
                        job['shellcommand']=command
                        self.commands.append(job)
                        seqid+=1
                    else:
                        raise(Exception("malformed keyval-command line %i in file %s: %s" % (n,file,line)))


    def start(self):
        self._manager.start()

    def run(self):

        ncommands=len(self.commands)
        if (ncommands > 0):
            #starting the server process
            shared_job_q = self._manager.get_job_q()
            shared_result_q = self._manager.get_result_q()

            #enqueuing command jobs
            for job in self.commands:
                shared_job_q.put(job)

            #waiting for results
            numresults = 0
            results = []
            nhosts = 0  #the number of executors
            while numresults < ncommands:
                outdict = shared_result_q.get()
                if (self.output == "stat"):
                    print(outdict)                           #TODO
                if (outdict["tag"] == "start"):
                    nhosts += 1
                    shared_job_q.put({"tag": "exit"})
                elif (outdict["tag"] == "jobdone"):
                    results.append(outdict)
                    numresults += 1
                else:
                    raise Exception("Unknown tag: '%s'" % outdict["tag"])

            #once here all jobs has been done
            time.sleep(1)  #give some time for a clean exit
            self._manager.shutdown()



class CommandBlockRunner:

    def __init__(self, manager_host, blockrunnerid=DEFAULT_BID, manager_port=DEFAULT_PORT, authkey=DEFAULT_AUTHKEY, runner_output=DEFAULT_RUNNEROUTPUT, runner_dir=DEFAULT_RUNNERDIR):
        self.blockrunnerid=blockrunnerid
        self.manager_port=manager_port
        self.manager_host=manager_host
        self.runner_output=runner_output
        self.runner_dir=runner_dir
        self.authkey=authkey
        self.manager = self._get_remote_manager(manager_host, manager_port, authkey)
        self.manager.connect()
        self.remote_job_q = self.manager.get_job_q()
        self.remote_result_q = self.manager.get_result_q()


    def _get_remote_manager(self,ip, port, authkey):
        class ServerQueueManager(BaseManager):
            pass
        ServerQueueManager.register('get_job_q')
        ServerQueueManager.register('get_result_q')
        return(ServerQueueManager(address=(ip, port), authkey=authkey))

    def _runner(self, job_q, result_q, idle_q, procid):
        while True:
            idle_q.put({'tag':'free'})
            job=job_q.get()
            if (job['tag'] == "exit"):
                break
            elif (job['tag'] == "cmd"):
                #extend the dictionary with result and id fields
                commandid=job["commandid"]
                _stdout=None
                _stderr=None
                if (self.runner_output == "file"):
                    outf = self.runner_dir + "/" + self.blockrunnerid + "." + commandid + ".outerr"
                    _stdout=open(outf,"w")
                    _stderr=subprocess.STDOUT
                elif(self.runner_output == "collect"):
                    _stdout=None
                    _stderr=None
                elif(self.runner_output == "null"):
                    _stdout=subprocess.DEVNULL
                    _stderr=subprocess.DEVNULL
                else:
                    raise Exception("Unknown runner_output: %s" % (self.runner_output,))
                try:
                    p = subprocess.Popen(job['shellcommand'], shell=True, stdout=_stdout, stderr=_stderr)
                    #pcompleted=subprocess.run(job['shellcommand'],shell=True,timeout=job['timeout'],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    #pcompleted=subprocess.run(job['shellcommand'],shell=True,timeout=job['timeout'])   AHHHHHHHHHHHHHHHHHHHHHH
                    returncode = p.wait(timeout=job['timeout'])
                except subprocess.TimeoutExpired:
                    job['timeouted']=True
                    job['retval']=None
                    p.kill()
                else:
                    job['timeouted']=False
                    #job['retval'] = pcompleted.returncode
                    job['retval'] = returncode

                job['procid']=procid
                job['tag']="jobdone"
                result_q.put(job)
            else:
                raise Exception("Unknown job tag: '%s'" % job["tag"])

    def _commandretriever(self, job_q, idle_q, job_count, job_outofstock, nprocs):
        while True:
            job = self.remote_job_q.get()
            if (job["tag"]=="exit"):
                job_outofstock.value=True
                for _ in range(0, nprocs):
                    job_q.put(job)
                break
            elif (job["tag"]=="cmd"):
                job_count.value+=1  #read+write, this is not atomic but I'm the only one to write it, so it's ok
                idle_q.get()        #be sure there is a free worker
                job_q.put(job)      #give him something to do

    def run(self, nprocs):

        #create
        if (self.runner_output == "file"):
          pathlib.Path(self.runner_dir).mkdir(parents=True,exist_ok=True)

        #properties of this blockrunner
        hosttags={"hostname": socket.gethostname(),"multirunnerid": self.blockrunnerid, "nprocs": nprocs}

        #inter-block shared objects
        job_q = multiprocessing.SimpleQueue()
        result_q = multiprocessing.SimpleQueue()
        idle_q = multiprocessing.SimpleQueue()
        job_count = multiprocessing.Value('i',0)
        result_count = multiprocessing.Value('i',0)
        job_outofstock = multiprocessing.Value('i',False)

        #start the retriever
        workers = []
        p_retriever = multiprocessing.Process(
            target=self._commandretriever,
            args=(job_q, idle_q, job_count, job_outofstock, nprocs))
        try:
            p_retriever.start()

            #start workers
            for i in range(nprocs):
                p = multiprocessing.Process(
                        target=self._runner,
                        args=(job_q, result_q,idle_q,i))
                p.start()
                workers.append(p)

            #comunicate to the master that we are starting
            self.remote_result_q.put({"tag":"start"})

            #send back results
            while True:
                oos=job_outofstock.value  #save it now because it could change during next "if condition" evaluation
                if (result_count.value < job_count.value):
                    result=result_q.get()
                    result.update(hosttags)
                    self.remote_result_q.put(result)
                    result_count.value+=1  #I'm the only one to increment this
                elif (oos):
                    break
                else:
                    time.sleep(1)

        except Exception:
            pass

        finally:
            #just in case
            for p in workers:
                p.terminate()
            p_retriever.terminate()


class RunFabric:
    def __init__(self, managerhost=socket.gethostname(), managerport=DEFAULT_PORT, sshhosts=None, ppn=multiprocessing.cpu_count(), timeout=DEFAULT_TIMEOUT, output=DEFAULT_OUTPUT, runner_output=DEFAULT_RUNNEROUTPUT, runner_dir=DEFAULT_RUNNERDIR):
        self.sshhosts=sshhosts
        self.ppn=ppn
        self.managerport=managerport
        self.managerhost=managerhost
        self.timeout=timeout
        #TODO: give a better logic to output options, and check it on argparse
        self.output=output
        if (output=="collect"):
            self.runner_output="collect"
        else:
            self.runner_output=runner_output
        self.runner_dir=runner_dir

    @classmethod
    def get_authkey(cls):
        strkey="%s-%s" % (datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'), os.getpid())
        return(strkey.encode('UTF-8'))

    def run(self, commandsfiles):
        authkey=self.get_authkey()  #different auth for each run
        runner_dir = self.runner_dir
        manager = CommandDispatcher(port=self.managerport, authkey=authkey, timeout=self.timeout, output=self.output)
        try:
            manager.load_commands(commandsfiles)
        except Exception as e:
            print("ABORT run: commandfile(s) not loaded because of exception: %s" % (str(e)),file=sys.stderr)
            return 1
        manager.start()
        if (self.sshhosts==None):
            cc=CommandBlockRunner(manager_host="localhost", blockrunnerid=socket.gethostname(), manager_port=self.managerport,
                                  authkey=authkey, runner_output=self.runner_output, runner_dir=runner_dir)
            ccp=multiprocessing.Process(target=cc.run,args=(self.ppn,))
            ccp.start()
        else:
            thismodule = (os.path.realpath(__file__))
            for sh in self.sshhosts:
                sp=multiprocessing.Process(target=subprocess.run,
                                           args=(["ssh", sh, thismodule,"runblock","--authkey",authkey,"--dispatcher_host", self.managerhost,"--nproc", str(self.ppn),
                                                  "--runner_output", self.runner_output, "--runner_dir", runner_dir],))
                sp.start()
        manager.run()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Utility to distribute shell command execution")
    subparsers=parser.add_subparsers(dest="subcommand", description='valid subcommands', help='choose the action to perform')
    subparsers.required=True
    parser_rundispatcher=subparsers.add_parser("rundispatcher",help="run locally only a dispatcher agent")
    parser_runfabric=subparsers.add_parser("runfabric",help="run an complete fabric: a locally dispatcher and local/remote blockrunner(s)")
    parser_runblock=subparsers.add_parser("runblock",help="run locally only a block agent")
    parser_cleanup=subparsers.add_parser("cleanup",help="killall local/remote dispatcher and blockrunner(s)")

    for p in [parser_rundispatcher,parser_runfabric,parser_runblock,parser_cleanup]:
        p.add_argument("--dispatcher_host", default="localhost")
        p.add_argument("--dispatcher_port", type=int, default=DEFAULT_PORT)

    for p in [parser_rundispatcher,parser_runfabric,parser_runblock]:
        p.add_argument("--nproc", type=int, default=DEFAULT_PPB)

    for p in [parser_rundispatcher, parser_runfabric]:
        p.add_argument("commandfile", metavar="COMMANDFILE", nargs='+', type=argparse.FileType('r', encoding='UTF-8'),
                       help="file containing a shell command specification per line")
        p.add_argument("--output", choices=["stat","null","collect"], default=DEFAULT_OUTPUT,
                       help="control what to capture/print")
        p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                       help="timeout to apply to commands, 0 is no timeout. Anyway it can be singularly overwritten by each command specification line. Default is %s." % (DEFAULT_TIMEOUT,))

    for p in [parser_runfabric,parser_runblock]:
        p.add_argument("--runner_output", choices=["collect","file","null"], default=DEFAULT_RUNNEROUTPUT,
                       help="controls what to do with runners stdout/stderr")
        p.add_argument("--runner_dir", default=DEFAULT_RUNNERDIR,
                       help="indicates the dir to use for runner output/debug files, it can be a directory in the same fs shared among blockrunners")

    for p in [parser_rundispatcher,parser_runblock]:
        p.add_argument("--authkey", default=None,
                        help="key to use for blockrunner/dispatcher authentication. The default is %s" % (DEFAULT_AUTHKEY.decode('UTF-8')))

    for p in [parser_runfabric,parser_cleanup]:
        p.add_argument("--host", nargs='+',help="ssh hosts where to start blockrunner")

    parser_runblock.add_argument("--blockrunnerid",default=DEFAULT_BID,
                                 help="Identifier that will be associated to each command execution. Useful in case of multiple block runner on the same host. The default is the hostname")
    parser_runfabric.add_argument("--authkey", default=None,
                                  help="it will be used as authkey. The default is in the form <TIMESTAMP>.<DISPATCHERPID>")

    args=parser.parse_args()
    if (args.subcommand == "runblock"):
        ak=args.authkey
        if (args.authkey is None):
            ak=DEFAULT_AUTHKEY
        else:
            ak=ak.encode('UTF-8')  #if present authkey must be codified
        cp=CommandBlockRunner(manager_host=args.dispatcher_host, blockrunnerid=args.blockrunnerid, manager_port=args.dispatcher_port, authkey=ak, runner_output=args.runner_output, runner_dir=args.runner_dir)
        #cp=CommandBlockRunner(manager_host=args.dispatcher_host,blockrunnerid=args.blockrunnerid,manager_port=args.dispatcher_port,authkey=args.authkey)
        cp.run(args.nproc)
    elif(args.subcommand == "rundispatcher"):
        ak=args.authkey
        if (args.authkey is None):
            ak = DEFAULT_AUTHKEY
        else:
            ak = ak.encode('UTF-8')  # if present authkey must be codified
        cd = CommandDispatcher(port=args.dispatcher_port, authkey=ak, timeout=args.timeout, output=args.output)
        cd.load_commands([f.name for f in args.commandfile])
        cd.start()
        cd.run()
    elif(args.subcommand == "runfabric"):
        cf=RunFabric(managerhost=args.dispatcher_host, managerport=args.dispatcher_port, sshhosts=args.host, ppn=args.nproc,
                     timeout=args.timeout, output=args.output, runner_output=args.runner_output, runner_dir=args.runner_dir)
        cf.run(commandsfiles=[f.name for f in args.commandfile])

#TODO: cambiare terminate in kill, creare runner_dir e runner_basedir