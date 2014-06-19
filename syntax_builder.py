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

from graph import Graph,ext_zero,ext_inc,ext_special,Dependency
from graph import CoNLLFormat, formats


inf=666 # integer to represent "infinity"




class NgramBuilder(object):
    """ Class to build syntactic ngrams. Following the same format as in: 
        http://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html
    """

    def __init__(self,queueIN,queuesOUT,data):
        self.queueIN=queueIN
        self.out_queues=queuesOUT # dict
        self.treeCounter=0


    def create_text_from_path(self,path,graph):
        """ Create a text format ngram from given graph and path.
            Path is a list of dependencys.
        """
        root=None
        tokens=[]
        last=None # dep idx
        deps=[] # list of (govIndex,dtype) tuples
        for tok in path:
            if (last is not None) and (last!=tok.dep): # last is ready, save
                text,morpho=graph.giveNode(last) # morpho=(lemma,pos,feat)
                lemma,pos,feat=morpho
                govs=u",".join(str(d[0]) for d in deps)
                deprels=u",".join(d[1] for d in deps)
                s=u"/".join(i for i in [text,lemma,pos,feat,deprels,govs])
                tokens.append(s)
                last=None
                deps=[]
            if tok.gov==-1: # this is root
                root=graph.nodes[tok.dep]
                govIndex=0
            else:
                govIndex=None # TODO: not efficient!
                for i in xrange(len(path)):
                    if path[i].dep==tok.gov:
                        govIndex=i+1
                        break
                if govIndex is None:
                    raise KeyError
            if last is None:
                last=tok.dep
            deps.append((govIndex,tok.type))
        else:
            if last:
                text,morpho=graph.giveNode(last)
                lemma,pos,feat=morpho
                govs=u",".join(str(d[0]) for d in deps)
                deprels=u",".join(d[1] for d in deps)
                s=u"/".join(i for i in [text,lemma,pos,feat,deprels,govs])
                tokens.append(s)  
        return root+u"\t"+u" ".join(t for t in tokens)+u"."+unicode(self.treeCounter)+unicode(random.randint(100,999))


    def expand_by_one(self,arcs,graph):
        """ For a set of unique arcs try to add one more dependency.
            Arcs is a set of dependency lists.
        """
        exp_arcs=set()
        for arc in arcs: # now arc is a list of dependencies, sorted because of comparison
            arc=list(arc) # now arc is a list again
            tokens=set([d.gov for d in arc if d.gov!=-1])
            tokens|=set([d.dep for d in arc if d.dep!=-1]) # collect all tokens from this arc except root
            for tok in tokens: # ...for each token in this arc
                # try to attach one dependency which is not part of arc
                dependencies=graph.deps[tok] # all dependents of this particular token
                for dep in dependencies: # TODO: functional markers
                    if dep not in arc: # not part of arc
                        new_arc=arc[:]
                        new_arc.append(dep)
                        new_arc.sort()
                        exp_arcs.add(tuple(new_arc))
        return exp_arcs
                

    def buildNgrams(self,graph):
        """ Build all ngrams of length biarcs to quadarcs"""
        arcs=set()
        for idx in range(len(graph.nodes)):
            types=graph.govs[idx]
            if not types: # this is a root token
                l=[Dependency(-1,idx,u"ROOT")]
                arcs.add(tuple(l))
            else:
                for d in types:
                    if (d.type in ext_zero) or (d.type in ext_inc) or (d.type in ext_special): continue # filter out if dtype is one of those functional markers...
                    l=[Dependency(-1,idx,d.type)]
                    arcs.add(tuple(l))
        ngrams=[]
        for arc in arcs: # nodes
            ngrams.append(self.create_text_from_path(arc,graph))
        self.db_batches[u"nodes"]+=ngrams
#        for n in ngrams:
#            print "node:", n
        for data in [u"arcs",u"biarcs",u"triarcs"]: # arcs---quadarcs
            ngrams=[]
            arcs=self.expand_by_one(arcs,graph)
            for arc in arcs:
                ngrams.append(self.create_text_from_path(arc,graph))
            self.db_batches[data]+=ngrams
#            for n in ngrams:
#                print data, n
        # TODO: quadarcs
        



    def process_sentence(self,sent):
        """ Create ngrams from one sentence. """
        graph=Graph.create(sent) # create new graph representation
        self.buildNgrams(graph) # create nodes--quadarcs


    def run(self):
        self.db_batches={} # key:dataset, value: list of ngrams
        for d in [u"nodes",u"arcs",u"biarcs",u"triarcs"]:
            self.db_batches[d]=[]
        while True:
            sentences=self.queueIN.get() # fetch a list of sentences from queue
            if not sentences: # end signal
                for key,val in self.db_batches.iteritems(): # send last batches
                    if val:
                        self.out_queues[key].put(val)             
                print >> sys.stderr, "builder process ending, returning"
                return
            for sent in sentences: # TODO: filter out parsebank markers + empty sentences
                #try:
                #print ">>>>>>>", u" ".join(tok[1] for tok in sent), "<<<<<<<"
                self.process_sentence(sent)
                #except:
                #    print >> sys.stderr, "error in processing sentence"
                #    traceback.print_exc()
                #    sys.stderr.flush()
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




