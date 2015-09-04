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

from graph import Graph,Dependency
from config import ext_zero,ext_inc,ext_special,coords,coord_conjs
from graph import CoNLLFormat, formats



class NgramBuilder(object):
    """ Class to build syntactic ngrams. Following the same format as in: 
        http://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html
    """

    def __init__(self,queueIN,queuesOUT,data,print_type):
        self.queueIN=queueIN
        self.out_queues=queuesOUT # dict
        self.print_type=print_type
        self.treeCounter=0


    def create_text_from_path(self,path,graph,prefix):
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
            if last is not None:
                text,morpho=graph.giveNode(last)
                lemma,pos,feat=morpho
                govs=u",".join(str(d[0]) for d in deps)
                deprels=u",".join(d[1] for d in deps)
                s=u"/".join(i for i in [text,lemma,pos,feat,deprels,govs])
                tokens.append(s)
        if self.print_type:
            return prefix+u"\t"+root+u"\t"+u" ".join(t for t in tokens)
        else:
            return root+u"\t"+u" ".join(t for t in tokens)

    def extended(self,path,graph):
        inc=set()
        spe=set()
        tokens=set([d.gov for d in path if d.gov!=-1 and d.type not in ext_inc])
        tokens|=set([d.dep for d in path if d.type not in ext_inc])
        for tok in tokens: # for each token in the path...
            deps=graph.deps[tok]
            for d in deps: # find all dependents
                if (d.dep not in tokens):
                    if (d.type in ext_special): # this is something new plus extended
                        spe.add(d)
                    if (d.type in ext_inc):
                        if d.type in coord_conjs: # include cc only if conj is present
                            for d2 in deps: # cc and conj must have the same governor...
                                if d2.type in coords and d2.dep in tokens:
                                    inc.add(d)
                                    break
                        else:
                            inc.add(d)
        return inc,spe


    def create_quadarcs(self,triarcs,graph):
        quadarcs=set()
        for arc,token in triarcs:
            arc=list(arc)
            tokens=set()
            for d in arc:
                tokens.add(d.dep)
                tokens.add(d.gov)
            deps=graph.deps[token]
            for d in deps:
                if d.dep not in tokens: # because we want only 'basic' quadarcs, allow this dep if d.dep not in path
                    new_arc=arc[:]
                    new_arc.append(d)
                    new_arc.sort()
                    quadarcs.add(tuple(new_arc))
            
        return quadarcs

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
                for dep in dependencies:
                    if (dep.type in ext_zero) or (dep.type in ext_inc) or (dep.type in ext_special):
                        continue 
                    if dep not in arc: # not part of arc
                        new_arc=arc[:]
                        new_arc.append(dep)
                        new_arc.sort()
                        exp_arcs.add(tuple(new_arc))
        return exp_arcs

    def filter_triarcs(self,triarcs):
        """ Filter the set of triarcs to have only those we need for building quadarcs. """
        filtered=set()
        for arc in triarcs:
            arc=list(arc)
            root=None
            first=set()
            sec=set()
            sec_head=set()
            for d in arc: # TODO: this is very unefficient way of doing this...
                if d.gov==-1: # this is arc root
                    root=d.dep
            for d in arc:
                if d.gov==-1:
                    continue
                if d.gov==root:
                    first.add(d.dep)
            for d in arc:
                if d.gov==-1 or d.dep in first:
                    continue
                if d.gov in first: # can be only one of these
                    sec.add(d.dep)
                    sec_head.add(d.gov)
                    break
            if len(first)==2 and len(sec)==1: # this is what we want
                token=first-sec_head
                assert len(token)==1
                filtered.add((tuple(arc),token.pop()))
        return filtered
                

    def buildNgrams(self,graph):
        """ Build all ngrams of length biarcs to quadarcs. """
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
        ext_ngrams=[]
        for arc in arcs: # nodes
            inc,spe=self.extended(arc,graph)
            a=list(arc)+list(inc)
            a.sort()
            ngrams.append(self.create_text_from_path(a,graph,u"nodes"))
            a=a+list(spe)
            a.sort()
            ext_ngrams.append(self.create_text_from_path(a,graph,u"ext_nodes"))
        self.db_batches[u"nodes"]+=ngrams
        self.db_batches[u"extended-nodes"]+=ext_ngrams
#        for n in ngrams:
#            print "node:", n
        for data in [u"arcs",u"biarcs",u"triarcs"]: # arcs---quadarcs
            ngrams=[]
            ext_ngrams=[]
            arcs=self.expand_by_one(arcs,graph)
            for arc in arcs:
                inc,spe=self.extended(arc,graph)
                a=list(arc)+list(inc)
                a.sort()
                ngrams.append(self.create_text_from_path(a,graph,data))
                a=a+list(spe)
                a.sort()
                ext_ngrams.append(self.create_text_from_path(a,graph,u"ext_"+data))
            self.db_batches[data]+=ngrams
            self.db_batches[u"extended-"+data]+=ext_ngrams
            if data==u"triarcs": # use these to create quadarcs
                filtered=self.filter_triarcs(arcs)
                quadarcs=self.create_quadarcs(filtered,graph)
                ngrams=[]
                ext_ngrams=[]
                for arc in quadarcs:
                    inc,spe=self.extended(arc,graph)
                    a=list(arc)+list(inc)
                    a.sort()
                    ngrams.append(self.create_text_from_path(a,graph,u"quadarcs"))
                    a=a+list(spe)
                    a.sort()
                    ext_ngrams.append(self.create_text_from_path(a,graph,u"ext_quadarcs"))
#                for n in ngrams:
#                    print n
                self.db_batches[u"quadarcs"]+=ngrams
                self.db_batches[u"extended-quadarcs"]+=ext_ngrams
#            for n in ngrams:
#                print data, n
        



    def process_sentence(self,sent,format=u"conllu"):
        """ Create ngrams from one sentence. """
        graph=Graph.create(sent,format) # create new graph representation
        self.buildNgrams(graph) # create nodes--quadarcs


    def run(self):
        self.db_batches={} # key:dataset, value: list of ngrams
        for d in u"nodes arcs biarcs triarcs quadarcs extended-nodes extended-arcs extended-biarcs extended-triarcs extended-quadarcs".split():
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
                if sent[0][1].startswith(u"####FIPBANK"): continue # skip parsebank markers
                if len(sent)==1 and sent[0][1]==u"": continue # skip whitespace sentences
                try:
                    self.process_sentence(sent)
                except:
                    print >> sys.stderr, "error in processing sentence"
                    traceback.print_exc()
                    sys.stderr.flush()
                self.treeCounter+=1 # this is needed for unique identifiers
            for key,val in self.db_batches.iteritems():
                if len(val)>5000:
                    self.out_queues[key].put(val)
                    self.db_batches[key]=[]


class ArgBuilder(object):
    """ Class to build verb and noun args. Following (almost) the same format as in: 
        http://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html
        Differences:
        - include also puntuation
        - include lemma and morphology for each token
    """

    def __init__(self,in_q,verb_q,noun_q,print_type):
        self.in_q=in_q
        self.form=formats[u"conllu"] # TODO define this properly
        self.verb_q=verb_q
        self.noun_q=noun_q
        self.print_type=print_type
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
        return root+u"\t"+u" ".join(t for t in tokens)



    def process_sent(self,sent):
        """ Create all verb and noun args from one sentence. """
        tree=collections.defaultdict(lambda:[]) # indexed with integers
        v_args=[]
        n_args=[]
        for line in sent: # first create dictionary, key:token, value:list of its dependents
            tok=int(line[self.form.ID])
            govs=line[self.form.HEAD].split(u",") # this is for second layer
            deprels=line[self.form.DEPREL].split(u",")
            if self.form.DEPS is not None and line[self.form.DEPS]!=u"_": #conllu DEPS field handling
                for gov_deprel in line[self.form.DEPS].split(u"|"):
                    gov,deprel=gov_deprel.split(u":",1)
                    govs.append(gov)
                    deprels.append(deprel)
            for gov,deprel in zip(govs,deprels):
                gov=int(gov)
                if gov==0: # skip root
                    continue
                if sent[gov-1][self.form.POS] in (u"V",u"N",u"VERB",u"NOUN"): # yes, we want this one
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
            if sent[root-1][self.form.POS] in (u"V",u"VERB"): # check where to store this one
                if self.print_type:
                    ngram=u"verb_arg\t"+ngram
                v_args.append(ngram)
            else:
                if self.print_type:
                    ngram=u"noun_arg\t"+ngram
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




