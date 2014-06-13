from collections import namedtuple

# extended types
ext_zero=u"prep".split() ## TODO Finnish types
ext_inc=u"cc".split()
ext_special=u"det poss neg aux auxpass ps mark complm prt".split()

CoNLLFormat=namedtuple("CoNLLFormat",["ID","FORM","LEMMA","POS","FEAT","HEAD","DEPREL"])
#Column lists for the various formats
formats={"conll09":CoNLLFormat(0,1,2,4,6,8,10)}

class Graph():

    @classmethod
    def init_ready(cls,n,e,d): # TODO do I ever need this?
        g=cls()
        g.nodes=n  ## list
        g.edges=e    ## list
        g.weights=d  ## dict
        return g

    @classmethod
    def create(cls,sent,conll_format="conll09"): 
        """ This is the way to create graphs! """
        g=cls()
        form=formats[conll_format] #named tuple with the column indices
        for line in sent:
            token=line[form.FORM].lower() # lowercase everything # TODO option to use lemmas
            gov=int(line[form.HEAD])
            g.addNode(token,line[form.POS]) # create a node to represent this token, store also pos TODO feat
            if int(gov)==0:
                continue
            dtype=line[form.DEPREL] # dependency type
            g.addEdge(gov-1,int(line[form.ID])-1,dtype) # TODO remove '-1'
        return g

    def __init__(self):
        """ Initialize empty, everything indexed as integers """
        self.nodes=[]
        self.edges=[]
        self.weights={}
        self.pos=[]
        self.syntax={}
#        self.deps=collections.defaultdict(lambda:[])


    def addNode(self,node,pos):
        self.nodes.append(node)
        self.pos.append(pos)


    def addEdge(self,u,v,dType):
        """
        u - gov index
        v - dep index
        """
        self.edges.append((u,v))
        # handle extended
        if (dType in ext_zero): # jump over these
            self.weights[(u,v)]=0
        elif (dType in ext_inc): # never include these when creating the path
            self.weights[(u,v)]=66
        elif (dType in ext_special): # or these
            self.weights[(u,v)]=66
        else:
            self.weights[(u,v)]=1
        self.syntax[v]=(u,dType)
#        self.deps[u].append((v,dType))


    def giveNode(self,node):
        try: ## TODO fix this, artificial root ?
            gov,dep=self.syntax[node]
        except KeyError:
            gov,dep=666,u"ROOT"
        return self.nodes[node],self.pos[node],gov,dep


    def isEmpty(self):
        if len(self.nodes)>0: return False
        else: return True

#    def giveExtended(self,path):
#        ext=[]
#        for p in path:
#            for idx,dType in self.deps[p]:
#                if dType in self.ext_special:
#                    ext.append(idx)
#        return ext



