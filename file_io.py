
import tarfile
import gzip
import codecs
import cStringIO as stringIO
import traceback
import sys
import os.path
import glob
import subprocess # needed for pigz

try:
    import leveldb
except:
    print >> sys.stderr, "LevelDB not installed, cannot use DBWriter."

class TarReader(object):

    def __init__(self,queue,fName,sample=None):
        self.queue=queue
        self.fName=fName
        self.sample=sample


    def run(self):
        """ Read .tar containing gzipped conll files. """
        counter=0
        tar=tarfile.open(self.fName)
        for tarInfo in tar:
            if not tarInfo.name.endswith(u".gz"):
                continue
            print >> sys.stdout, tarInfo.name
            try:
                rawF=tar.extractfile(tarInfo)
                rawContent=rawF.read()
                rawF.close()
                # put raw content into queue
                self.queue.put(rawContent,True,None)
                counter+=1
            except:
                print >> sys.stderr, "Skipping", tarInfo.name
                traceback.print_exc()
                sys.stderr.flush()
            tar.members=[] #This is *critical* to prevent tar from accumulating the untarred files and eating all memory!
            if (self.sample is not None) and (counter>=self.sample):
                break
        tar.close()
        print >> sys.stderr, "Tar reader done,",counter,"files sent to the queue"
        sys.stderr.flush()
                
        #self.DBwriter.write_batches() # write all remaining batches to the DBs



class FileReader(object):

    def __init__(self,queue,batch_size):
        self.queue=queue
        self.batch_size=batch_size
        self.totalCount=0

    def read(self,inp,processes):
        """ inp can be one file or directory with multiple files. """
        if os.path.isdir(inp): # inp is directory
            files=glob.glob(os.path.join(inp,"*.gz"))
            files.sort()
            for fName in files:
                self.read_file(fName)
        elif inp.endswith(u".gz") or inp.endswith(u".conll"): # inp is a gzip or conll file
            self.read_file(inp)
        else:
            raise ValueError(u"Wrong input format.")
        #Signal end of work to all processes (Thanks @radimrehurek and @fginter for this neat trick!)
        for _ in range(processes):
            self.queue.put(None)
        print >> sys.stderr, "File reader ready, returning"
        print >> sys.stderr, "Total number of sentences: "+str(self.totalCount)
        return

    def readGzip(self,fName):
        """ Uses multithreaded gzip implementation (pigz) to read gzipped files. """
        p=subprocess.Popen(("pigz","--decompress","--to-stdout","--processes","2",fName),stdout=subprocess.PIPE,stdin=None,stderr=subprocess.PIPE)
        return p.stdout


    def read_file(self,fName):
        """ Reads one .gz file and puts sentences into queue. """
        print >> sys.stderr, fName
        if fName.endswith(u".gz"):
            f=codecs.getreader("utf-8")(self.readGzip(fName))
            # f=codecs.getreader("utf-8")(gzip.open(fName)) # TODO use this as a fallback
        else:
            f=codecs.open(fName,u"rt",u"utf-8")
        sentences=[]
        for sent in self.read_conll(f):
            sentences.append(sent)
            self.totalCount+=1
            if len(sentences)>self.batch_size:
                self.queue.put(sentences)
                sentences=[]
        else:
            if sentences:
                self.queue.put(sentences)
        f.close()
        


    def read_conll(self,f):
        """ Reads conll and yields one sentence at a time. """
        # TODO skip parsebank identifiers!
        sent=[]
        for line in f:
            line=line.strip()
            if not line or line.startswith(u"#"): #Do not rely on empty lines in conll files, ignore comments
                continue 
            if line.startswith(u"1\t") and sent: #New sentence, and I have an old one to yield
                yield sent
                sent=[]
            sent.append(line.split(u"\t"))
        else:
            if sent:
                yield sent


class DBWriter(object):

    def __init__(self,queue,out_dir,dataset,cutoff):
        self.dataset=dataset
        self.queue=queue
        self.outdir=out_dir
        self.DB,self.batch=self.createDB(dataset)
        self.cutoff=cutoff


    def createDB(self,dataset):
        db=leveldb.LevelDB(self.outdir+u"/"+dataset+u".leveldb",create_if_missing=True)
        batch=leveldb.WriteBatch()
        return db,batch


    def run(self):
        while True:
            ngram_list=self.queue.get() # fetch new batch
            if not ngram_list: # end signal
                print >> sys.stderr, "no new data in "+self.dataset+", creating final text file"
                sys.stderr.flush()
                try:
                    c=self.create_final_files() # create .gz text file
                except:
                    print >> sys.stderr, "error while creating final text file: "+self.dataset+" ,returning"
                    sys.stderr.flush()
                    return
                print >> sys.stderr, c,self.dataset,"written, returning"
                sys.stderr.flush()
                return
            try:
                batch=leveldb.WriteBatch() # write new batch
                for ngram in ngram_list:
                    batch.Put(ngram.encode(u"utf-8"),u"1".encode(u"utf-8"))
                self.DB.Write(batch)
            except:
                print >> sys.stderr, "error in database writer, batch rejected: "+self.dataset
                traceback.print_exc()
                sys.stderr.flush()      


    def create_final_files(self):
        """ Go through the database and collect counts to final text file. """
        f=gzip.open(self.outdir+u"/"+self.dataset+u".gz",u"w")
        totalCounter=0
        last=None
        count=0
        for key,value in self.DB.RangeIter(None,None):
            key=unicode(key,u"utf-8")
            ngram,ident=key.rsplit(u".",1) # split ngram and identifier
            if last!=ngram and count>0:
                if count>=self.cutoff:
                    f.write((last+u"\t"+unicode(count)+u"\t2014,"+unicode(count)+u"\n").encode(u"utf-8")) # write to the file
                    totalCounter+=1
                last=ngram
                count=1
            elif last is None:
                last=ngram
                count=1
            else:
                count+=1
        else:
            if count>0 and count>=self.cutoff:
                f.write((last+u"\t"+unicode(count)+u"\t2014,"+unicode(count)+u"\n").encode(u"utf-8")) # write last one  
        f.close()
        return totalCounter

class FileWriter(object):

    def __init__(self,queue,out_dir,dataset):
        self.dataset=dataset
        self.queue=queue
        self.outdir=out_dir
        self.file_out=codecs.getwriter("utf-8")(gzip.open(self.outdir+u"/"+dataset+u".txt.gz","w"))


    def run(self):
        while True:
            ngram_list=self.queue.get() # fetch new batch
            if not ngram_list: # end signal
                print >> sys.stderr, "no new data in "+self.dataset
                sys.stderr.flush()
                self.file_out.close()
                return
            try:
                for ngram in ngram_list:
                    print >> self.file_out, ngram
            except:
                print >> sys.stderr, "error in file writer, batch rejected: "+self.dataset
                traceback.print_exc()
                sys.stderr.flush()


class StdoutWriter(object):

    def __init__(self,queue):
        self.queue=queue

    def run(self):
        while True:
            ngram_list=self.queue.get() # fetch new batch
            if not ngram_list: # end signal (None)
                return
            for ngram in ngram_list:
                print >> sys.stdout, ngram
            sys.stdout.flush()


