import multiprocessing
import Queue
import argparse
import os
import sys

from file_io import TarReader, DBWriter, FileReader
from syntax_builder import SyntaxNgramBuilder
            

def launch(args):

    data=u"nodes arcs biarcs triarcs".split()    

    # if output directory does not exist, create it
    if not os.path.exists(args.output):
        os.makedirs(args.output)
    else: # delete old databases ?
        pass
        #for d in data:
        #    os.system("rm -rf "+args.output+"/"+d+".leveldb")
        #    os.system("rm -rf "+args.output+"/"+d+".gz")


    procs=[] # gather all processes together

    ## gz file queue
    data_q=multiprocessing.Queue(args.processes*2) # max items in a queue at a time

    ## file reader process (just one) TODO: read also tar, sample size
    reader=FileReader(data_q,50)
    readerProcess=multiprocessing.Process(target=reader.read, args=(args.input[0],args.processes))
    readerProcess.start()
    procs.append(readerProcess)

    ## ngram queues, separate queue for each dataset
    ngram_queues={}
    for dataset in data:
        ngram_queues[dataset]=multiprocessing.Queue(15) # max items in a queue at a time

    ## ngram builder processes (parallel)
    print >> sys.stderr, "Launching",args.processes,"ngram builder processes"
    for _ in range(args.processes):
        builder=SyntaxNgramBuilder(data_q,ngram_queues,data) # TODO do something smarter with 'data'
        builderProcess=multiprocessing.Process(target=builder.run)
        builderProcess.start()
        procs.append(builderProcess)
  
    w_procs=[]
  
    ## separate database writer for each dataset
    print >> sys.stderr, "Launching",len(data),"database writer processes"
    for d in data:
        writer=DBWriter(ngram_queues[d],args.output,d)
        writerProcess=multiprocessing.Process(target=writer.run)
        writerProcess.start()
        w_procs.append(writerProcess)

    for p in procs:
        p.join() # wait reader and builder processes to quit
    
    # now reader and builders are ready
    # send end signal for each DB writer (Thanks @radimrehurek and @fginter for this neat trick!)
    for dataset in data:
        ngram_queues[dataset].put(None)

    for p in w_procs:
        p.join() # and wait


if __name__==u"__main__":

    parser = argparse.ArgumentParser(description='Build syntactic ngrams in a multi-core setting.')
    g=parser.add_argument_group("Input/Output")
    g.add_argument('input', nargs=1, help='Name of the input dir.')
    g.add_argument('-o', '--output', required=True, help='Name of the output dir.')
    g=parser.add_argument_group("Builder config")
    g.add_argument('-p', '--processes', type=int, default=4, help='How many builder processes to run? (default %(default)d)')
    #g.add_argument('--max_sent', type=int, default=0, help='How many sentences to read from the input? 0 for all.  (default %(default)d)')
    args = parser.parse_args()

    
    launch(args)

    # TODO:
    # - add command line argument for data sets
    # - add morpho


    # TODO:
    # -always include cc (and prep?)
    # -extended ngrams
    # -Finnish types to ext_
    # -quadarcs logic
    # -artificial root for tree (and remove KeyError)
    # -remove whitespace sentences

    ## When you run this remember:
    ## 1) ulimit -n 4096
    ## 2) nice -n 19 python build_ngrams.py

    



