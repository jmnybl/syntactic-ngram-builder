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

from graph import Graph,ext_zero,ext_inc,ext_special
from graph import CoNLLFormat, formats


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


class NgramBuilder(object):
    """ Class to build syntactic ngrams. Following the same format as in: 
        http://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html
    """

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



    def buildNgrams(self,graph,n,dataset,dist,next): # TODO: we need to build bi- and trigrams at once, cause trigrams are 'expanded' bigrams...
        """ Build all ngrams of length n """
        ngrams=[]
        for u in xrange(0,len(graph.nodes)):
            for v in xrange(u+1,len(graph.nodes)): # we can treat this as a triangular matrix
                if dist[u][v]==n: # TODO: take only if length is 2 
                    p=self.path(u,v,next)
                    p=p.split(u"-")
                    for i in xrange(0,len(p)): # convert into int
                        p[i]=int(p[i])
#                    if dataset.startswith(u"ext"):
#                        ngrams+=graph.giveExtended(p)
                    text_ngram=self.create_text_from_path(p,graph) # here we create all bigrams then
                    if text_ngram is not None:
                        ngrams.append(text_ngram)
                    # TODO: and now it's time to expand bigrams with one dependency to get trigrams, collect a tree dictionary (key:token, value:all its dependents), then for each node in bigram, try to attach a new dependent, finally take a set of all expansions to get unique trigrams (index nodes with tree indexes)
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


class ArgBuilder(object):
    """ Class to build verb and noun args. Following (almost) the same format as in: 
        http://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html
        Differences:
        - include also puntuation
        - include lemma and morphology for each token
    """

    def __init__(self,in_q,verb_q,noun_q):
        self.in_q=in_q
        self.form=formats[u"conll09"] # TODO define this properly
        self.verb_q=verb_q
        self.noun_q=noun_q
        self.treeCounter=0


    def extract_ngram(self,root_idx,deps,sent):
        """
            root_idx - index of a ngram root token in the sentence
            deps - dependents of a ngram root, (idx,dtype) list
        """
        deps.append((root_idx,None)) # add root to get correct word order
        deps.sort() # sort this to get tokens in correct order
        r=None # root index in ngram
        for i in xrange(0,len(deps)):
            if deps[i][0]==root_idx:
                r=i+1
                break
        assert (r is not None)
        root=None
        tokens=[]
        for idx,dtype in deps:
            text,lemma,POS,feat=sent[idx-1][self.form.FORM].lower(),sent[idx-1][self.form.LEMMA].lower(),sent[idx-1][self.form.POS],sent[idx-1][self.form.FEAT] # take also lemma and morpho
            if idx==root_idx: # this is root
                root=text.lower()
                govIndex=0
                dtype=sent[root_idx-1][self.form.DEPREL] # take as is, may have multiple types but it does not matter
            else:
                govIndex=r
            s=u"/".join(i for i in [text,lemma,POS,feat,dtype,unicode(govIndex)])
            tokens.append(s)
        return root+u"\t"+u" ".join(t for t in tokens)+u"."+unicode(self.treeCounter)+unicode(random.randint(100,999)) # unique identifier



    def process_sent(self,sent):
        """ Create all verb and noun args from one sentence. """
        tree=collections.defaultdict(lambda:[]) # indexed with integers
        v_args=[]
        n_args=[]
        for line in sent: # first create dictionary, key:token, value:list of its dependents
            tok=int(line[self.form.ID])
            govs=line[self.form.HEAD].split(u",") # this is for second layer
            deprels=line[self.form.DEPREL].split(u",")
            for gov,deprel in zip(govs,deprels):
                gov=int(gov)
                if gov==0: # skip root
                    continue
                if sent[gov-1][self.form.POS]==u"V" or sent[gov-1][self.form.POS]==u"N": # yes, we want this one
                    tree[gov].append((tok,deprel))
        # now we need to take care of cases where the verb has same dependents listed there twice with different dtype (e.g. rels)
        for root,dependents in tree.iteritems():
            uniq_deps=list(set([d for d,t in dependents])) # now we have a list of uniq dependents
            deps=[]
            for dep in uniq_deps:
                dtypes=u",".join(t for d,t in dependents if d==dep)
                deps.append((dep,dtypes))
            # now deps is a list of unique dependents populated with dependency types
            ngram=self.extract_ngram(root,deps,sent) # create text ngram
            if sent[root-1][self.form.POS]==u"V": # check where to store this one
                v_args.append(ngram)
            else:
                n_args.append(ngram)
        self.v_batch+=v_args
        self.n_batch+=n_args
            


    def build(self):
        """ Fetch data from queue send it forward. """
        self.v_batch=[]
        self.n_batch=[]
        while True:
            sentences=self.in_q.get() # fetch new sentences
            if not sentences: # end signal, time to stop
                if self.v_batch:
                    self.verb_q.put(self.v_batch)
                if self.n_batch:
                    self.noun_q.put(self.n_batch)
                print >> sys.stderr, "no new data, "+str(self.treeCounter)+" sentences processed, arg builder ends"
                return
            for sent in sentences:
                if len(sent)>1: # no need to process sentences with lenght 1
                    try:
                        self.process_sent(sent)
                        self.treeCounter+=1
                    except:
                        traceback.print_exc()
                        sys.stderr.flush()
            if len(self.v_batch)>100: # add batches to the queue
                self.verb_q.put(self.v_batch)
                self.v_batch=[]
            if len(self.n_batch)>100:
                self.noun_q.put(self.n_batch)
                self.n_batch=[]




