#!/usr/bin/env python
"""
DMLC submission script by ssh

One need to make sure all slaves machines are ssh-able.
"""
from __future__ import absolute_import
from time import sleep
import os, subprocess, logging, math
from threading import Thread
from . import tracker

def sync_dir(local_dir, slave_node, slave_dir):
    """
    sync the working directory from root node into slave node
    """
    remote = slave_node[0] + ':' + slave_dir
    logging.info('rsync %s -> %s', local_dir, remote)
    prog = 'rsync -az --rsh="ssh -o StrictHostKeyChecking=no -p %s" %s %s' % (
        slave_node[1], local_dir, remote)
    subprocess.check_call([prog], shell = True)

def get_env(pass_envs):
    envs = []
    # get system envs
    keys = ['OMP_NUM_THREADS', 'KMP_AFFINITY', 'LD_LIBRARY_PATH', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            envs.append('export ' + k + '=' + v + ';')
    # get ass_envs
    for k, v in pass_envs.items():
        envs.append('export ' + str(k) + '=' + str(v) + ';')
    return (' '.join(envs))

def submit(args):
    assert args.host_file is not None
    with open(args.host_file) as f:
        tmp = f.readlines()
    assert len(tmp) > 0
    hosts=[]
    infs = []
    for h in tmp:
        if len(h.strip()) > 0:
            # parse addresses of the form ip:port
            h = h.strip()
            if h.startswith('#'):
                continue
            #i = h.find(":")
            #p = "22"
            #if i != -1:
            #    p = h[i+1:]
            #    h = h[:i]
            # hosts now contain the pair ip, port
            
            segments = h.split(':')            
            if len(segments) == 1:
                p = "22"
                c = "mlx4_0"
            elif len(segments) == 2:
                p = "22"
                c = segments[1]
                h = segments[0]
            else:
                p = segments[2]
                c = segments[1]
                h = segments[0]
                
            hosts.append((h, p))
            infs.append(c)

    def loadEnvIfAny(passEnvs, queryKeys):
        for k in queryKeys:
            if k in os.environ:
                passEnvs[k] = os.environ[k]
    def ssh_submit(nworker, nserver, pass_envs):
        """
        customized submit script
        """
        # thread func to run the job
        def run(prog):
            subprocess.check_call(prog, shell = True)

        # sync programs if necessary
        local_dir = os.getcwd()+'/'
        working_dir = local_dir
        if args.sync_dst_dir is not None and args.sync_dst_dir != 'None':
            working_dir = args.sync_dst_dir
            for h in hosts:
                sync_dir(local_dir, h, working_dir)
        # PHUB launch jobs
        IB_QP_COUNT = 'IB_QP_COUNT'
        IB_DIRECT_CONNECT = 'IB_DIRECT_CONNECT'
        DMLC_ROLE = 'DMLC_ROLE'
        DISABLE_HEARTBEAT = 'DISABLE_HEARTBEAT'
        DMLC_TRACKER_TOTAL_ID = 'DMLC_TRACKER_TOTAL_ID'
        PSLITE_VAN_TYPE = 'PSLITE_VAN_TYPE'
        PSHUB_DATA_THREAD_COUNT = 'PSHUB_DATA_THREAD_COUNT'
        MXNET_ENGINE_TYPE = 'MXNET_ENGINE_TYPE'
        PHUB_SUPPRESS_AGGREGATOR = 'PHUB_SUPPRESS_AGGREGATOR'
        PHUB_SUPPRESS_OPTIMIZER = 'PHUB_SUPPRESS_OPTIMIZER'
        PHUB_PREFETCH_DIST = 'PHUB_PREFETCH_DIST'
        PHUB_OPTIMIZER = 'PHUB_OPTIMIZER'
        PHUB_AGGREGATOR = 'PHUB_AGGREGATOR'
        PHUB_ASYNC_MODE = 'PHUB_ASYNC_MODE'
        PHUB_BITWIDTH_SIM = 'PHUB_BITWIDTH_SIM'
        MXNET_KVSTORE_BIGARRAY_BOUND = 'MXNET_KVSTORE_BIGARRAY_BOUND'
        DMLC_SERVER_ID = 'DMLC_SERVER_ID'
        DMLC_WORKER_ID = 'DMLC_WORKER_ID'
        DMLC_INTERFACE = 'DMLC_INTERFACE'
        MXNET_CPU_PRIORITY_NTHREADS = 'MXNET_CPU_PRIORITY_NTHREADS'
        MXNET_CPU_WORKER_NTHREADS = 'MXNET_CPU_WORKER_NTHREADS'
        PS_VERBOSE = 'PS_VERBOSE'
        PHUB_PREFERRED_INTERFACE = 'PHUB_PREFERRED_INTERFACE'
        PHUB_BLOCKED_INTERFACE = 'PHUB_BLOCKED_INTERFACE'
        loadEnvIfAny(pass_envs, [PHUB_PREFERRED_INTERFACE, PHUB_PREFETCH_DIST, PHUB_OPTIMIZER, PHUB_AGGREGATOR, PHUB_BITWIDTH_SIM, PHUB_SUPPRESS_AGGREGATOR,IB_DIRECT_CONNECT, IB_QP_COUNT, PHUB_SUPPRESS_AGGREGATOR, DMLC_ROLE, PHUB_ASYNC_MODE, DISABLE_HEARTBEAT,PS_VERBOSE,  PSLITE_VAN_TYPE, PSHUB_DATA_THREAD_COUNT, MXNET_ENGINE_TYPE, PHUB_SUPPRESS_OPTIMIZER, MXNET_KVSTORE_BIGARRAY_BOUND, DMLC_INTERFACE, MXNET_CPU_PRIORITY_NTHREADS, MXNET_CPU_WORKER_NTHREADS])
        # launch jobs. only for central PHUB
        PHUB_COLOCATION_POLICY = 'PHUB_COLOCATION_POLICY'
        ColocationPolicy = 'DEFAULT'
        if PHUB_COLOCATION_POLICY in os.environ:
            ColocationPolicy = os.environ[PHUB_COLOCATION_POLICY]
        if PSLITE_VAN_TYPE in os.environ and os.environ[PSLITE_VAN_TYPE] == 'pshub' and nserver == 1:
            #PSHuB Requires no worker to be co-located with it.
            #first, launch server
            pass_envs[DMLC_ROLE] = 'server'
            pass_envs[DISABLE_HEARTBEAT] = 'TRUE' #heartbeat is really annoying
            pass_envs[DMLC_SERVER_ID] = 0
            pass_envs[DMLC_TRACKER_TOTAL_ID] = 0
            loadEnvIfAny(pass_envs, [PHUB_BLOCKED_INTERFACE])

            (node,port) = hosts[0]
            prog = get_env(pass_envs) + ' cd ' + working_dir + '; ' + (' '.join(args.command))
            prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' -p ' + port + ' \'' + prog + '\''
            #print "info warning note error prog is " + prog
            #todo: ugly
            thread = Thread(target = run, args=(prog,))
            thread.setDaemon(True)
            thread.start()
            #sleep(1)
            
            if PHUB_BLOCKED_INTERFACE in pass_envs:
                del pass_envs[PHUB_BLOCKED_INTERFACE]
            for i  in range(nworker):
                pass_envs[DMLC_ROLE] = 'worker'
                pass_envs[DMLC_WORKER_ID] = i
                pass_envs[DMLC_TRACKER_TOTAL_ID] = i + 1
                pass_envs[PHUB_PREFERRED_INTERFACE] = infs[(i % (len(hosts) - 1)) + 1]
                (node,port) = hosts[(i % (len(hosts) - 1)) + 1]
                prog = get_env(pass_envs) + ' cd ' + working_dir + '; ' + (' '.join(args.command))
                prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' -p ' + port + ' \'' + prog + '\''
                thread = Thread(target = run, args=(prog,))
                thread.setDaemon(True)
                thread.start()
                #sleep(1)
                
        elif ColocationPolicy == 'DEFAULT' or ColocationPolicy == 'COLOCATE':
            eMachine = len(hosts)
            if ColocationPolicy == 'COLOCATE':
                eMachine = len(hosts) if nworker > len(hosts) else nworker
                print "Using Collocated " + str(eMachine) + " machines"
            for i in range(nworker + nserver):
                #print "|------------------------------------------------------->  launching ",i
                pass_envs[DMLC_ROLE] = 'server' if i < nserver else 'worker'
                pass_envs[DISABLE_HEARTBEAT] = 'TRUE'
                pass_envs[DMLC_TRACKER_TOTAL_ID] = i
                pass_envs[PHUB_PREFERRED_INTERFACE] = infs[i % eMachine]
                if i < nserver:
                    pass_envs[DMLC_SERVER_ID] = i
                else:
                    pass_envs[DMLC_WORKER_ID] = i - nserver
                (node, port) = hosts[i % eMachine]
                prog = get_env(pass_envs) + ' cd ' + working_dir + '; ' + (' '.join(args.command))
                prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' -p ' + port + ' \'' + prog + '\''
                thread = Thread(target = run, args=(prog,))
                thread.setDaemon(True)
                thread.start()
                #sleep(1)
        elif ColocationPolicy == 'NO_COLOCATE':
            totalMachine = len(hosts)
            workerMachine = int(math.ceil(totalMachine * (1.0 * nworker / (nworker + nserver))))
            if totalMachine == workerMachine:
                workerMachine -= 1
                if workerMachine == 0:
                    assert False, "NO_COLOCATE policy cannot be enforced with this strategy"
            serverMachine = totalMachine - workerMachine
            #assert False
            #launch workers
            for i in range(nworker):
                pass_envs[DMLC_ROLE] = 'worker'
                pass_envs[DISABLE_HEARTBEAT] = 'TRUE'
                pass_envs[DMLC_TRACKER_TOTAL_ID] = i
                pass_envs[DMLC_WORKER_ID] = i
                pass_envs[PHUB_PREFERRED_INTERFACE] = infs[i % workerMachine]
                (node,port) = hosts[i % workerMachine]
                prog = get_env(pass_envs) + ' cd ' + working_dir + '; ' + (' '.join(args.command))
                prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' -p ' + port + ' \'' + prog + '\''
                thread = Thread(target = run, args=(prog,))
                thread.setDaemon(True)
                thread.start()
                #sleep(1)
            #launch servers
            for i in range(nserver):
                pass_envs[DMLC_ROLE] = 'server'
                pass_envs[DISABLE_HEARTBEAT] = 'TRUE'
                pass_envs[DMLC_TRACKER_TOTAL_ID] = nworker + i
                pass_envs[DMLC_SERVER_ID] = i
                pass_envs[PHUB_PREFERRED_INTERFACE] = infs[i % serverMachine + workerMachine]
                (node,port) = hosts[i % serverMachine + workerMachine]
                prog = get_env(pass_envs) + ' cd ' + working_dir + '; ' + (' '.join(args.command))
                prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' -p ' + port + ' \'' + prog + '\''
                thread = Thread(target = run, args=(prog,))
                thread.setDaemon(True)
                thread.start()
                #sleep(1)

        else:
            #always colocate.
            #first, spread workers.
            assert False, "Unknown PHUB_COLOCATE_POLICY " + os.environ[PHUB_COLOCATE_POLICY]
                

        return ssh_submit

    tracker.submit(args.num_workers, args.num_servers,
                   fun_submit=ssh_submit,
                   pscmd=(' '.join(args.command)))
