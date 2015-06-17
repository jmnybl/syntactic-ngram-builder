import multiprocessing
import Queue
import argparse
import os
import sys

from file_io import FileReader, FileWriter, StdoutWriter
from syntax_builder import NgramBuilder, ArgBuilder
            

def launch_ngrams(args):

    data=u"nodes arcs biarcs triarcs quadarcs extended-nodes extended-arcs extended-biarcs extended-triarcs extended-quadarcs".split()    

    # if output directory does not exist, create it
    if args.out_dir and not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    procs=[] # gather all processes together

    ## data queue
    data_q=multiprocessing.Queue(args.processes*2) # max items in a queue at a time

    ## file reader process (just one) to populate data queue
    reader=FileReader(data_q,100,args.max_sent_len) # number of sentences per one batch
    readerProcess=multiprocessing.Process(target=reader.read, args=(args.input[0],args.processes))
    readerProcess.start()
    procs.append(readerProcess)

    ## ngram queues, separate queue for each dataset
    ngram_queues={}
    if args.stdout:
        q=multiprocessing.Queue(25) # max items in a queue at a time
        for dataset in data:
            ngram_queues[dataset]=q
    else:
        for dataset in data:
            ngram_queues[dataset]=multiprocessing.Queue(25)

    ## ngram builder processes (parallel)
    print >> sys.stderr, "Launching",args.processes,"ngram builder processes"
    for _ in range(args.processes):
        builder=NgramBuilder(data_q,ngram_queues,data,args.stdout) # TODO do something smarter with 'data'
        builderProcess=multiprocessing.Process(target=builder.run)
        builderProcess.start()
        procs.append(builderProcess)
  
    w_procs=[]
  
    
    
    if args.stdout: # just one stdout writer process needed
        print >> sys.stderr, "Launching 1 stdout writer process"
        writer=StdoutWriter(ngram_queues["nodes"])
        writerProcess=multiprocessing.Process(target=writer.run)
        writerProcess.start()
        w_procs.append(writerProcess)
    else: ## separate file writer for each dataset
        print >> sys.stderr, "Launching",len(data)," file writer processes"
        for d in data:
            writer=FileWriter(ngram_queues[d],args.out_dir,d)
            writerProcess=multiprocessing.Process(target=writer.run)
            writerProcess.start()
            w_procs.append(writerProcess)

    for p in procs:
        p.join() # wait reader and builder processes to quit before continuing
    
    # send end signal for each DB writer (Thanks @radimrehurek and @fginter for this neat trick!)
    if args.stdout:
        ngram_queues["nodes"].put(None)
    else:
        for dataset in data:
            ngram_queues[dataset].put(None)

    for p in w_procs:
        p.join() # and wait

def launch_args(args):

    #data=u"nodes arcs biarcs triarcs quadarcs".split()    

    # if output directory does not exist, create it
    if args.out_dir and not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)


    procs=[] # gather all processes together

    ## data queue
    data_q=multiprocessing.Queue(args.processes*2) # max items in a queue at a time

    ## file reader process (just one) to populate data queue
    reader=FileReader(data_q,50,args.max_sent_len) # number of sentences per one batch
    readerProcess=multiprocessing.Process(target=reader.read, args=(args.input[0],args.processes))
    readerProcess.start()
    procs.append(readerProcess)

    ## args queues
    if args.stdout:
        verb_q=multiprocessing.Queue(15)
        noun_q=verb_q # we need just one of these
    else:
        verb_q=multiprocessing.Queue(15) # max items in a queue at a time
        noun_q=multiprocessing.Queue(15)

    ## builder processes (parallel)
    print >> sys.stderr, "Launching",args.processes,"args builder processes"
    for _ in range(args.processes):
        builder=ArgBuilder(data_q,verb_q,noun_q,args.stdout)
        builderProcess=multiprocessing.Process(target=builder.build)
        builderProcess.start()
        procs.append(builderProcess)
  
    w_procs=[]
  
    
    
    if args.stdout:
        print >> sys.stderr, "Launching 1 stdout writer processes"
        writer=StdoutWriter(verb_q)
        writerProcess=multiprocessing.Process(target=writer.run)
        writerProcess.start()
        w_procs.append(writerProcess)
    else: ## separate file writer for each dataset
        print >> sys.stderr, "Launching 2 file writer processes"
        writer=FileWriter(verb_q,args.out_dir,"verb_args")
        writerProcess=multiprocessing.Process(target=writer.run)
        writerProcess.start()
        w_procs.append(writerProcess)
        writer=FileWriter(noun_q,args.out_dir,"noun_args")
        writerProcess=multiprocessing.Process(target=writer.run)
        writerProcess.start()
        w_procs.append(writerProcess)

    for p in procs:
        p.join() # wait reader and builder processes to quit before continuing
    
    # send end signal for each DB writer (Thanks @radimrehurek and @fginter for this neat trick!)
    if args.stdout:
        verb_q.put(None)
    else:
        verb_q.put(None)
        noun_q.put(None)
        
    for p in w_procs:
        p.join() # and wait


if __name__==u"__main__":

    parser = argparse.ArgumentParser(description='Build syntactic ngrams in a multi-core setting.')
    g=parser.add_argument_group("Config")
    g.add_argument('input', nargs=1, help='Name of the input dir or file. Supported formats are .gz and .conll files (using CoNLL09 format)')
    g.add_argument('-p', '--processes', type=int, default=4, help='How many builder processes to run? (default %(default)d)')
    #g.add_argument('--cutoff', type=int, default=2, help='Frequency threshold, how many times an ngram must occur to be included? (default %(default)d)')
    g.add_argument('--ngrams', action='store_true', help='Build syntactic ngrams (nodes---quadarcs)')
    g.add_argument('--args', action='store_true', help='Build syntactic arguments (verb and noun args)')
    g=parser.add_mutually_exclusive_group(required=True)
    g.add_argument('--out_dir', help='Print ngrams to .gz files, give the name of the output directory, where .gz files get written. Mutually exclusive with --stdout.')
    g.add_argument('--stdout', action='store_true', help='Print ngrams to stdout. Mutually exclusive with --out_dir.')
    g.add_argument('--max-sent-len', action='store', type=int, default=256, help='Skip sentences longer than this (prevents memory excursions on extremely long trees). (default %(default)d)')
    
    args = parser.parse_args()


    if args.args:
        print >> sys.stderr, "Building verb and noun args..."
        launch_args(args)

    if args.ngrams:
        print >> sys.stderr, "Building ngrams..."
        launch_ngrams(args)
    
    if not args.ngrams and not args.args:
        print >> sys.stderr, "Use either --ngrams (for syntactic ngrams ) or --args (for verb and noun args)."
        sys.exit(1)

    



