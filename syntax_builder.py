# -*- coding: utf-8 -*-

import numpy
import sys
import random
import os
import collections
import traceback
import sys
import cStringIO as stringIO
import gzip

from graph import Graph


inf=666 # integer to represent "infinity"

data_table={
u"nodes":0,
u"arcs":1,
u"biarcs":2,
u"triarcs":3,
u"quadarcs":4,
u"extended-nodes":0,
u"extended-arcs":1,
u"extended-biarcs":2,
u"extended-triarcs":3,
u"extended-quadarcs":4}

# extended types
ext_zero=u"prep".split() ## TODO Finnish types
ext_inc=u"cc".split()
ext_special=u"det poss neg aux auxpass ps mark complm prt".split()


class SyntaxNgramBuilder():
    """ Class to build Google Syntax Ngrams from Conll format trees. """

    def __init__(self,queueIN,queuesOUT,data):
        self.queueIN=queueIN
        self.out_queues=queuesOUT
        self.datasets={} # key: dataset_name, value: n
        for d in data:
            self.datasets[d]=data_table[d]
        self.treeCounter=0


    def path(self,i,j,next):
        """ Reconstruct the shortest path between i and j. """
        if i==j:
            return str(i) # TODO format
        if next[i][j]==-1:
            raise NameError("No path between i and j.")
        intermediate=next[i][j]
        return self.path(i,intermediate,next) + "-"+str(j) # TODO format


    def floydWarshall(self, graph):
        """ Floyd-Warshall algorithm to find all-pairs shortest paths. """
        size=len(graph.nodes)    
        # init dist array
        dist=numpy.empty([size,size],dtype=int)
        dist.fill(inf)
        for i in xrange(0,size):
            dist[i][i]=0
        for u,v in graph.edges:
            dist[u][v]=graph.weights[(u,v)]  # the weight of the edge (u,v)
            dist[v][u]=graph.weights[(u,v)]  # this will treat the graph as undirected
        # init next array
        next=numpy.empty([size,size],dtype=int)
        next.fill(inf)
        for i in xrange(0, size):
            for j in xrange(0, size):
                if i==j or dist[i][j]==inf:
                    next[i][j]=-1
                else:
                    next[i][j]=i
        for k in xrange(0, size):
            for i in xrange(0, size):
                for j in xrange(0, size):
                    if dist[i][j]>dist[i][k]+dist[k][j]:
                        dist[i][j]=dist[i][k]+dist[k][j]
                        next[i][j]=next[k][j]

        return dist,next


    def create_text_from_path(self,path,graph):
        """ Create a text format ngram from given graph and path. """
        path.sort() # sort this to get tokens in correct order
        root=None
        tokens=[]
        for tok in path:
            text,POS,gov,dType=graph.giveNode(tok)
            if gov not in path:
                if (dType in ext_zero) or (dType in ext_inc) or (dType in ext_special):
                    return None # skip if root is a functional-marker
                root=text
                govIndex=0
            else:
                govIndex=path.index(gov)+1
            s=u"/".join(i for i in [text,POS,dType,unicode(govIndex)])
            tokens.append(s)
        return root+u"\t"+u" ".join(t for t in tokens)+u"."+unicode(self.treeCounter)+unicode(random.randint(100,999)) # TODO unique identifier



    def buildNgrams(self,graph,n,dataset,dist,next):
        """ Build all ngrams of length n """
        ngrams=[]
        for u in xrange(0,len(graph.nodes)):
            for v in xrange(u+1,len(graph.nodes)): # we can treat this as a triangular matrix
                if dist[u][v]==n:
                    p=self.path(u,v,next)
                    p=p.split(u"-")
                    for i in xrange(0,len(p)): # convert into int
                        p[i]=int(p[i])
#                    if dataset.startswith(u"ext"):
#                        ngrams+=graph.giveExtended(p)
                    text_ngram=self.create_text_from_path(p,graph)
                    if text_ngram is not None:
                        ngrams.append(text_ngram)
        self.db_batches[dataset]+=ngrams


    def create_nodes(self,dataset,graph):
        ngrams=[]
        for i in xrange(0, len(graph.nodes)):
            p=[i]
#            if dataset.startswith(u"ext"):
#                p+=graph.giveExtended(p)
            text_ngram=self.create_text_from_path(p,graph)
            if text_ngram is not None:
                ngrams.append(text_ngram)
        self.db_batches[dataset]+=ngrams   


    def processGraph(self,graph):
        dist,next=self.floydWarshall(graph)
        for data in self.datasets:
            if data==u"nodes" or data==u"extended-nodes":
                self.create_nodes(data,graph)
            else:
                n=self.datasets[data]
                self.buildNgrams(graph,n,data,dist,next)



    def process_sentence(self,sent):
        """ Create ngrams from one sentence. """
        graph=Graph.create(sent) # create new graph representation
        self.processGraph(graph)


    def run(self):
        self.db_batches={} # key:dataset, value: list of ngrams
        for d in self.datasets:
            self.db_batches[d]=[]
        while True:
            sentences=self.queueIN.get() # fetch a list of sentences from queue
            if not sentences: # end signal
                for key,val in self.db_batches.iteritems(): # send last batches
                    if val:
                        self.out_queues[key].put(val)             
#                for d in self.datasets:
#                    self.out_queues[d].put(None) # send end signal for each database writer process
                print >> sys.stderr, "builder process ending, returning"
                return
            for sent in sentences:
                try:
                    self.process_sentence(sent)
                except:
                    print >> sys.stderr, "error in processing sentence"
                    traceback.print_exc()
                    sys.stderr.flush()
                self.treeCounter+=1 # this is needed for unique identifiers
            for key,val in self.db_batches.iteritems():
                if len(val)>1000:
                    self.out_queues[key].put(val)
                    self.db_batches[key]=[]

