"""
Contains:

Methods:
- timing(func)
- doc_gen(f_stream)

Classes:
- DocParser
- MetaData
- Lexicon
- InvIndex
- Tokenizer
- Query
"""
import time
import os
from collections import Counter
import xml.etree.ElementTree as ET
import string

import pickle
import gzip

import bisect
from porterstem import PorterStemmer

def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        lapsed_time = round(time2-time1, 3)
        print ('Time taken: {} seconds'.format(str(lapsed_time)))
        return ret
    return wrap

def doc_gen(fstream, start_tag = '<DOC>', end_tag = '</DOC>'):
    """ Takes a fstream of our scrapped data, latimes.gz,
    and yields one doc at a time.
    """
    doc = ''
    indoc = False # bool holds state of whether within a doc
    for line in fstream:
        #checking for <DOC> or <\DOC> depending on state of indoc
        if not indoc:
            # check for <DOC>
            if start_tag.lower() in line.lower():
                doc_ls = []
                indoc = True
        else: #currently reading a doc
            # check for </DOC>
            if end_tag.lower() in line.lower():
                indoc = False
                doc_ls.append(line.strip() + ' ')
                yield ''.join(doc_ls)
        if indoc:
            doc_ls.append(line.strip() + ' ')

class LightDocParser(object):
    """Used to simply break appart the dom tree for retrieval."""
    def __init__(self, doc, tags = [
                                'DOCNO',
                                'TEXT',
                                'HEADLINE',
                                'GRAPHIC']):
        self.tags = tags
        self.doc = doc
        self.tree = ET.fromstring(self.doc)
        self.cont_dict = {}
        self._fill_cont_dict()

    def _fill_cont_dict(self):
        """Parses doc for self.text_tags and stores content in self.cont_dict"""
        for node in self.tree:
            for tag in self.tags:
                if node.tag.lower() == tag.lower():
                    self.cont_dict[tag] = self.grab_tag_cont(node)

    def grab_tag_cont(self, node):
        """Grabs all content contained within a passed node"""
        #base case:
        cont = ''
        if len(node) < 1:
            #at a leaf:
            cont = node.text
        else:
            #need to traverse deeper
            for cnode in node:
                cont += self.grab_tag_cont(cnode)
        return cont

class DocParser(object):
    """Uses the xml lib to grab desired elements of the xml dom efficiently"""
    def __init__(self, doc, tags = [
                                'DOCNO',
                                'TEXT',
                                'HEADLINE',
                                'GRAPHIC'],
                            tokenize_tags = [
                                'TEXT',
                                'HEADLINE',
                                'GRAPHIC'
                            ]):
        self.tags = tags
        self.tokenize_tags = tokenize_tags
        self.doc = doc
        self.tree = ET.fromstring(self.doc)
        self.stemmer = PorterStemmer()
        self.cont_dict = {} #stores flattened tag content
        self._fill_cont_dict()
        self.pre_tokens = []
        self.tokens = []
        self._tokenize()
        self.doc_len = len(self.tokens)

    def _fill_cont_dict(self):
        """Parses doc for self.text_tags and stores content in self.cont_dict"""
        for node in self.tree:
            for tag in self.tags:
                if node.tag.lower() == tag.lower():
                    self.cont_dict[tag] = self.grab_tag_cont(node)

    def grab_tag_cont(self, node):
        """Grabs all content contained within a passed node"""
        #base case:
        cont = ''
        if len(node) < 1:
            #at a leaf:
            cont = node.text
        else:
            #need to traverse deeper
            for cnode in node:
                cont += self.grab_tag_cont(cnode)
        return cont

    def _tokenize(self, stem=False):
        """Taking cont_dict and tokenize_tags, update the self.tokens vector"""
        punc = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
        translator = str.maketrans('', '', punc)
        if not stem:
            for tag in self.tokenize_tags:
                if tag in self.cont_dict:
                    self.tokens += self.cont_dict[tag]\
                                            .lower()\
                                            .translate(translator)\
                                            .split()
        else:
            for tag in self.tokenize_tags:
                if tag in self.cont_dict:
                    self.tokens += self._stem_string(self.cont_dict[tag]\
                                            .lower()\
                                            .translate(translator))\
                                            .split()


    def _stem_string(self, input_str):
        """ Takes a string and stems it returning a stemmed string"""
        output = ''
        word = ''
        line = input_str
        for c in line:
            if c.isalpha():
                word += c.lower()
            else:
                if word:
                    output += self.stemmer.stem(word, 0,len(word)-1)
                    word = ''
                output += c.lower()
        return output


class MetaData(object):
    """Container for metadata of a given file"""
    def __init__(self,
                doc_path,
                docno=None,
                docid=None,
                date=None,
                hl=None,
                raw_doc=None,
                doc_len=None):
        self.doc_path = doc_path # full doc path
        #all the fields that will be serialized and written to file:
        self.docno = docno
        self.docid = docid
        self.date = date
        self.hl = hl
        self.raw_doc = raw_doc
        self.doc_len = doc_len


    def save(self):
        """Takes meta fields if all required are pop'd (not None)
        and writes them to file.
        """
        self._check_basepath_exists()
        file = gzip.GzipFile(self.doc_path, 'wb')
        file.write(pickle.dumps(self, protocol=0))
        file.close()
    #@timing
    def load(self):
        """Loads or reloads pickled object from meta store.
        sets self so no value returned
        """
        if self.doc_path == None:
            raise TypeError("Missing doc_path to load object.")
        file = gzip.GzipFile(self.doc_path, 'rb')
        buffer = bytes()
        count = 0
        while True:
                data = file.read()
                if data == b'':
                        break
                buffer += data
        object = pickle.loads(buffer)
        file.close()
        self._update_self(object)

    def _update_self(self, obj):
        """Takes new obj and reassigns the fields of current object."""
        self.docno = obj.docno
        self.docid = obj.docid
        self.date = obj.date
        self.hl = obj.hl
        self.raw_doc = obj.raw_doc
        self.doc_len = obj.doc_len

    # NOTE: Might want to create IO class later
    def _check_basepath_exists(self):
        """Check if basepath dir exists and if does not, creates it"""
        basepath = os.path.dirname(self.doc_path)
        #create basepath:
        if not os.path.exists(basepath):
            os.makedirs(basepath)

    def meta_print(self):
        """MetaData print function"""
        print(meta_print_str.format(
                        str(self.docno),
                        str(self.docid),
                        str(self.date),
                        str(self.hl),
                        str(self.doc_len),
                        self.raw_doc
            ))


meta_print_str =\
"""
docno: {}
internal id: {}
date: {}
headline: {}
document length: {}
raw document:
{}
"""

class Lexicon(object):
    """The Lexicon contains two mapping dicts:
        - term to term_id
        - term_id to term_id
    """
    def __init__(self, save_path, tokens=None):
        # required:
        self.save_path = os.path.join(save_path, 'lexicon')
        self.tokens = tokens
        self.term_2_termid = None
        self.termid_2_term = None

    def create_lexicon_mappings(self):
        """Takes a normalized token vector and updates dict mappings"""
        self.term_2_termid = {token: idx if not token.isdigit() else int(token)
             for idx, token in enumerate(set(self.tokens))}
        self.termid_2_term = inv_map = {v: k
             for k, v in self.term_2_termid.items()}
        return None

    def conv_tokens_vect(self, doc_tokens):
        """Converts a vector of terms to their termid's
        Returns: dict with termid:count
        """
        if type(doc_tokens) != list:
            raise TypeError("doc_tokens parameter needs to be a list")
        if self.term_2_termid == None:
            self.load()
        termid_vect = []
        for term in doc_tokens:
            if term in self.term_2_termid:
                termid_vect.append(self.term_2_termid[term])
        return Counter(termid_vect)

    def save(self):
        """Saves lexicon dicts to index dir"""

        file = gzip.GzipFile(self.save_path, 'wb')
        file.write(pickle.dumps(self, protocol=0))
        file.close()

    #@timing
    def load(self):
        """Loads or reloads pickled object from meta store.
        sets self so no value returned
        """
        if self.save_path == None:
            raise TypeError("Missing doc_path to load object.")
        file = gzip.GzipFile(self.save_path, 'rb')
        buffer = bytes()
        count = 0
        while True:
                data = file.read()
                if data == b'':
                        break
                buffer += data
        object = pickle.loads(buffer)
        file.close()
        self._update_self(object)

    def _update_self(self, obj):
        """Takes new obj and reassigns the fields of current object."""
        self.tokens = obj.tokens
        self.term_2_termid = obj.term_2_termid
        self.termid_2_term = obj.termid_2_term

class InvIndex(object):
    """Obj used to hold, alter and update the inverted index."""
    def __init__(self, save_path):
        self.save_path = os.path.join(save_path, 'InvertedIndex')
        self.inv_index = {} # dict that hold the inverted index
        self.coll_len = 0
        self.coll_token_sum = 0

    def add_term_posting(self, termid, docid, count):
        """Takes a posting and then adds it the existing inverted index"""
        posting = (docid, count)
        if termid in self.inv_index:
            # update postings list:
            self.update_postings_list(termid, posting)
        else:
            # new term:
            self.create_postings_list(termid, posting)

    def create_postings_list(self, termid, posting):
        """Given a termid"""
        self.inv_index[termid] = [
            [posting[0]],
            [posting[1]]
        ]

    def update_postings_list(self, termid, posting):
        """Given a termid, updates a correct posting list."""
        i = bisect.bisect(self.inv_index[termid][0], posting[0])
        self.inv_index[termid][0].insert(i, posting[0]) # docid insert
        self.inv_index[termid][1].insert(i, posting[1]) # count insert
        assert(len(self.inv_index[termid][0]) == len(self.inv_index[termid][1]))

    def does_termid_exist(self, termid):
        """Grabs a postings_list using the inverted index"""
        if int(termid) in self.inv_index:
            return True
        else:
            return False

    def get_posting_ls(self, termid):
        """Grabs a postings_list using the inverted index"""
        return self.inv_index[int(termid)]

    def save(self):
        """Saves lexicon dicts to index dir"""

        file = gzip.GzipFile(self.save_path, 'wb')
        file.write(pickle.dumps(self, protocol=0))
        file.close()

    #@timing
    def load(self):
        """Loads or reloads pickled object from meta store.
        sets self so no value returned
        """
        print("Loading inverted index")
        if self.save_path == None:
            raise TypeError("Missing doc_path to load object.")
        file = gzip.GzipFile(self.save_path, 'rb')
        buffer = bytes()
        count = 0
        while True:
                data = file.read()
                if data == b'':
                        break
                buffer += data
        object = pickle.loads(buffer)
        file.close()
        self._update_self(object)

    def _update_self(self, obj):
        """Takes new obj and reassigns the fields of current object."""
        print("Updating inv_index ref.")
        self.inv_index = obj.inv_index
        self.coll_len = obj.coll_len
        self.coll_token_sum = obj.coll_token_sum


class Tokenizer(object):
    """Class used to tokenize strings"""
    def __init__(self):
        self.punc = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
        self.translator = str.maketrans('', '', self.punc)
        self.stemmer = PorterStemmer()

    def tokenize(self, token_str, stem = False):
        """Taking cont_dict and tokenize_tags, update the self.tokens vector"""
        if not stem:
            return token_str.lower().translate(self.translator).split()
        else:
            return self._stem_string(token_str.lower()\
                            .translate(self.translator))\
                            .split()

    def _stem_string(self, input_str):
        """ Takes a string and stems it returning a stemmed string"""
        output = ''
        word = ''
        line = input_str
        for c in line:
            if c.isalpha():
                word += c.lower()
            else:
                if word:
                    output += self.stemmer.stem(word, 0,len(word)-1)
                    word = ''
                output += c.lower()
        return output



class Query(object):
    """Resposible for running all queries against a defined index working dir"""
    def __init__(self, index_wd):
        self.index_wd = index_wd
        self.lexicon = Lexicon(self.index_wd)
        self.lexicon.load()
        self.tokenizer = Tokenizer()
        self.invIndex = InvIndex(self.index_wd)
        self.invIndex.load()
        self.docid_to_docno_path = os.path.join(self.index_wd, 'docid_to_docno.p')
        self.docid_to_docno = pickle.load(open(self.docid_to_docno_path, "rb"))
        self.docno_to_data_path = os.path.join(self.index_wd, 'docno_to_data.p')
        self.docno_to_data = pickle.load(open(self.docno_to_data_path, "rb"))

    def tokenize(self, query_str):
        """ Takes query string and returns term tokens"""
        return self.tokenizer.tokenize(query_str)

    def conv_tokens_to_ids(self, tokens_vect):
        """Takes token vector and returns termid vector"""
        return list(self.lexicon.conv_tokens_vect(tokens_vect).keys())

    def BooleanAND(self, termid_vect):
        """Takes termid vector and returns list of intersecting docid's based
        on a BooleanAND retrieval.
        """
        postings_lists = {}
        for termid in termid_vect:
            if self.invIndex.does_termid_exist(termid):
                postings_lists[termid] = self.invIndex.get_posting_ls(termid)
            else:
                print("WARNING: {} not found.".format(str(termid)))
        docids, counts = self.find_set_intersection(postings_lists)
        return docids, counts

    def general_retrieval(self, termid_vect):
        """Takes the termid_vect and grabs all documents and re
        Params:
        ------
        - termid_vect : list(int)
            input token id vect
        """
        postings_dict = {}
        postings_dict_tuples = {} # holds tuple pairs
        for termid in termid_vect:
            if self.invIndex.does_termid_exist(termid):
                postings_dict[termid] = self.invIndex.get_posting_ls(termid)
            else:
                print("WARNING: {} not found.".format(str(termid)))

        keys = list(postings_dict.keys())
        for inx, lists in enumerate(postings_dict.values()):

            postings_dict_tuples[keys[inx]] = []
            for j, post in enumerate(lists[0]):
                postings_dict_tuples[keys[inx]].append(
                    tuple([post, lists[1][j]])
                )
        return postings_dict_tuples

    def find_set_intersection(self, postings_lists):
        """Given multiple lists, finds the intersecting elements are return
        them
        """
        f_pl_docid = list(postings_lists.values())[0][0]
        f_pl_count = list(postings_lists.values())[0][1]
        docids = list(postings_lists.values())[0][0]
        for item in postings_lists.values():
            docids = list(set(docids) & set(item[0]))
        inx = []
        for posting in docids:
            for i, elem in enumerate(f_pl_docid):
                if str(elem) == str(posting):
                    inx.append(i)
                    break # stop once element is found
        #use ind to grab counts:
        T = [f_pl_count[i] for i in inx]

        return(docids, T)
        intersection = postings_lists.values()[0][0]
        for l in postings_lists.values():
            intersection = list(set(intersection) & set(l))
        return intersection

    def docno_from_docid(self, docid):
        """Takes docid and returns the docno"""
        return self.docid_to_docno[docid]
    def docid_to_metadata(self, docid):
        """Given a valid docid, returns the corresponding metadata object"""
        if docid not in self.docid_to_docno:
            raise KeyError("Docid {} not found in current index.".format(str(docid)))
        metadata_path = self.docno_to_data[self.docid_to_docno[docid]]
        metadata = MetaData(metadata_path)
        metadata.load()
        return metadata

    def docno_to_metadata(self, docno):
        """Given valid docno, returns related MetaData obj"""
        if docno not in self.docno_to_data:
            raise KeyError("Docno {} not found in current index.".format(str(docno)))
        metadata_path = self.docno_to_data[docno]
        metadata = MetaData(metadata_path)
        metadata.load()
        return metadata
