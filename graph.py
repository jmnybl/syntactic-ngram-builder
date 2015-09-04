from collections import namedtuple,defaultdict

CoNLLFormat=namedtuple("CoNLLFormat",["ID","FORM","LEMMA","POS","FEAT","HEAD","DEPREL","DEPS"])
#Column lists for the various formats
formats={"conll09":CoNLLFormat(0,1,2,4,6,8,10,None),"conllu":CoNLLFormat(0,1,2,3,5,6,7,8)}

# 0  1    2     3    4    5   6    7      8    9
#id form lemma cpos pos feat head deprel deps misc

class Dependency(object):
    """ Simple class to represent dependency. """

    def __init__(self,g,d,t):
        self.gov=g
        self.dep=d
        self.type=t

    def __eq__(self,other):
        return (self.gov==other.gov and self.dep==other.dep and self.type==other.type)

    def __cmp__(self,other): # do I ever need to sort list of deps?
        if self.dep<other.dep: return -1 # self smaller than other
        elif self.dep>other.dep: return 1
        else: # same gov
            if self.gov<other.gov: return -1
            elif self.gov>other.gov: return 1
            else: # also same dep
                if self.type<other.type: return -1
                elif self.type>other.type: return 1
                else: return -1 # the same, raise error?

    def __hash__(self):
        return hash(str(self.gov)+"-"+str(self.dep)+"-"+self.type)

    def __repr__(self):
        return (str(self.gov)+":"+str(self.dep)+":"+self.type)


class Graph(object):

    @classmethod
    def init_ready(cls,n,e,d): # TODO do I ever need this?
        g=cls()
        g.nodes=n  ## list
        g.edges=e    ## list
        g.weights=d  ## dict
        return g

    @classmethod
    def create(cls,sent,conll_format="conllu"): 
        """ This is the way to create graphs! """
        g=cls()
        form=formats[conll_format] #named tuple with the column indices
        for line in sent:
            token=line[form.FORM].lower() # lowercase everything
            g.addNode(token,(line[form.LEMMA],line[form.POS],line[form.FEAT])) # create a node to represent this token, store also pos, lemma and feat
            if conll_format=="conll09":
                govs=line[form.HEAD].split(u",")
                deprels=line[form.DEPREL].split(u",") # dependency types
                for gov,deprel in zip(govs,deprels):
                    if int(gov)==0:
                        continue
                    g.addEdge(int(gov)-1,int(line[form.ID])-1,deprel) # TODO remove '-1'
            elif conll_format=="conllu":
                if int(line[form.HEAD])!=0:
                    g.addEdge(int(line[form.HEAD])-1,int(line[form.ID])-1,line[form.DEPREL])
                if line[form.DEPS]!=u"_":
                    for gov_drel in line[form.DEPS].split(u"|"):
                        gov,drel=gov_drel.split(u":",1)
                        if int(gov)!=0:
                            g.addEdge(int(gov)-1,int(line[form.ID])-1,drel)
        return g

    def __init__(self):
        """ Initialize empty, everything indexed as integers """
        self.nodes=[]
        self.edges=[]
        #self.weights={}
        self.pos=[]
        self.govs=defaultdict(lambda:[]) # key: token, value: list of its governors
        self.deps=defaultdict(lambda:[]) # key: token, value: list of dependents
        self.dtypes=defaultdict(lambda:[]) # key: (gov,dep) tuple, value list of deptypes


    def addNode(self,node,morpho):
        self.nodes.append(node)
        self.pos.append(morpho)


    def addEdge(self,u,v,dtype):
        """
        u - gov index
        v - dep index
        """
        dep=Dependency(u,v,dtype)
        self.edges.append((u,v))
        self.govs[v].append(dep)
        self.deps[u].append(dep)
        self.dtypes[(u,v)].append(dtype)


    def giveNode(self,node):
        return self.nodes[node],self.pos[node]


    def isEmpty(self):
        if len(self.nodes)>0: return False
        else: return True





