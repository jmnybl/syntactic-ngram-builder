from collections import namedtuple

# extended types
ext_zero=u"prep".split() ## we don't have these in Finnish
ext_inc=u"cc adpos".split() ## adpos is always included because it's the Finnish version of prep
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
        self.weights={} # TODO: do we need something extra for rels?
        self.pos=[]
        self.govs=collections.defaultdict(lambda:[]) # key: token, value: list of its governors
        self.deps=collections.defaultdict(lambda:[]) # key: token, value: list of dependents
        seld.dTypes=collections.defaultdict(lambda:[]) # key: (gov,dep) tuple, value list of deptypes


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
        elif (dType in ext_inc): # never include these when creating the path, add separately
            self.weights[(u,v)]=66
        elif (dType in ext_special): # or these
            self.weights[(u,v)]=66
        else:
            self.weights[(u,v)]=1
        self.govs[v].append(u)
        self.deps[u].append(v)
        self.dtypes[(u,v)].append(dType)


    def giveNode(self,node):
        try: ## TODO fix this, artificial root ?
            govs=self.govs[node]
        except KeyError:
            gov,dep=666
        return self.nodes[node],self.pos[node],gov


    def isEmpty(self):
        if len(self.nodes)>0: return False
        else: return True





