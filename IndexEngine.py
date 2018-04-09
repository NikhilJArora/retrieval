"""Indexing Engine.
Author: Nikhil Arora (NikhilJArora@outlook.com)

This module is intended to provide a potential solution to a search engine.

Example
-------
literal blocks::
    $ python IndexEngine.py <document_to_index> <where_to_store_new_index>

    $ python IndexEngine.py /Users/nikhilarora/data/latimes/latimes.gz /Users/nikhilarora/data/latimes/index_dir_test
    $ python IndexEngine.py /Users/nikhilarora/data/latimes/latimes_sample.txt.gz /Users/nikhilarora/data/latimes/index_dir_sample
    $ python IndexEngine.py /Users/nikhilarora/data/latimes/latimes.gz /Users/nikhilarora/data/latimes/index_dir_baseline
    $ python IndexEngine.py /Users/nikhilarora/data/latimes/latimes.gz /Users/nikhilarora/data/latimes/index_dir_stem
"""
import os
import sys
import gzip
import re
import pickle
import itertools
from datetime import date
from index_helpers import (doc_gen,
                           timing,
                           DocParser,
                           MetaData,
                           Lexicon,
                           InvIndex)


def validate_args(args):
    if len(args) != 3:
        print("Incorrect number of arguments.")
        cli_help_msg()
        sys.exit()

    data_path = args[1]
    index_wd = args[2]
    #check the data_path exists
    if not os.path.isfile(data_path) or not data_path.endswith('.gz'):
        print('Current path: {} is an invalid path to latimes.gz.  Please provide \
                a valid path.'.format(data_path))
        print('Exiting program.')
        sys.exit()

    #check that index_wd does not exist
    if os.path.isdir(index_wd):
        print('Current dir: {} already exists and cannot be used to store the \
                new index.'.format(index_wd))
        print('Exiting program.')
        sys.exit()
# NOTE: Metastore currently takes 3 minutes to populate with current data
# full index process including dumping to disk:
@timing
def index_engine(data_path, index_wd):
    """Main entry to the index engine responsible for processing all the
    documents for fast and efficient retrieval at a later time.

    Parameters
    ----------
    data_path : str
        Path to the dataset being indexed.
    index_wd : str
        Unused path where index and org'd docs are to be stored.

    Returns
    -------
    None
    """
    print("Starting the indexing engine.")

    docno_to_data = {}
    docid_val = 0
    N = 0 # coll length
    coll_token_sum = 0

    docid_to_docno = {}
    tokens_dict = {} # dict of docid:tokens_ls

    # grab the file steam
    fstream = gzip.open(data_path, 'rt', encoding='utf-8')
    # main index loop.
    for doc in doc_gen(fstream):
        N += 1
        print("Current {docid_val}".format(docid_val=docid_val))
        print("Current doc has length: {}".format(len(doc)))

        docid_val += 1
        docid = docid_val
        doc_parser = DocParser(doc)
        docno = cln_docno(doc_parser.cont_dict['DOCNO'])
        if 'HEADLINE' in doc_parser.cont_dict:
            headline = doc_parser.cont_dict['HEADLINE']
        else:
            headline = ''
        date = get_date(docno)
        doc_len = doc_parser.doc_len
        coll_token_sum += doc_len
        print('summed coll_token_sum: {}'.format(str(coll_token_sum)))
        doc_path = get_doc_path(index_wd, docno)
        metadata = MetaData(doc_path,
                            docno=docno,
                            docid=docid,
                            date=date,
                            hl=headline,
                            raw_doc=doc,
                            doc_len=doc_len)
        metadata.save()
        docno_to_data[docno] = doc_path
        docid_to_docno[docid] = docno
        tokens_dict[docid] = doc_parser.tokens

    print("Flattening tokens list")
    flat_tokens_ls = itertools.chain.from_iterable(tokens_dict.values())
    print("Creating & saving Lexicon")
    lexicon = Lexicon(index_wd, tokens=flat_tokens_ls)
    lexicon.create_lexicon_mappings()
    lexicon.save()
    print("Creating & saving docno_to_data")
    pickle_obj(index_wd, 'docno_to_data', docno_to_data)
    pickle_obj(index_wd, 'docid_to_docno', docid_to_docno)

    invIndex = InvIndex(save_path=index_wd)
    invIndex.coll_len = N
    invIndex.coll_token_sum = coll_token_sum
    #using the created lexicon, we will now
    for docid, tokens_vect in tokens_dict.items():
        print("Building inv index: Current {docid_val}".format(docid_val=docid))
        # convert the doc token vectors using the lexicon
        termid_counts = lexicon.conv_tokens_vect(tokens_vect)
        for termid, count in termid_counts.items():
            invIndex.add_term_posting(termid, docid, count)

    print("Saving the inverted index")
    invIndex.save()



#-------------------------------------------------------------------------------
# Helper functions:

def cln_docno(r_docno):
    """clean any unwanted chars from the item and returns a clean string
    Example
    -------
    "<DOCNO> LA010189-0001 </DOCNO>" to "LA010189-0001"
    """
    cln_docno = None
    if r_docno is not None:
        cln_docno = r_docno.replace(' ', '')
    return cln_docno

def get_date(docno):
    """clean any unwanted chars from the item and returns a clean string"""
    month = int(docno[2:4])
    day = int(docno[4:6])
    year = int('19' + docno[6:8])
    return date(day=day, month=month, year=year).strftime('%B %d, %Y')

def get_doc_path(index_wd, docno):
    """ combines the index_wd (module lvl var) with docno to build path in
    the form of <index_wd>/metastore/YY/MM/DD/NNNN

    Notes
    -----
         - metadata will be stored at <index_wd>/YY/MM/DD/NNNN_meta.gz
         - full raw doc will be stored at <index_wd>/YY/MM/DD/NNNN_r.gz

    Parameters
    ----------
    docno : string
        expect format of string is "LAMMDDYY-NNNN" -->
    Returns
    -------
    string : "metastore/YY/MM/DD/NNNN.gz"
        basepath for meta and raw data to be stored and later retrieved.
    """
    YY = docno[6:8].strip()
    MM = docno[2:4].strip()
    DD = docno[4:6].strip()
    NNNN = docno[9:].strip()

    return os.path.join(index_wd, 'metastore',YY, MM, DD, NNNN)

# NOTE: Can update the method below to support compression writes...
def pickle_obj(index_wd, dict_name, dict):
    """Will persist obj to disk for retriaval and usage later."""
    pickle_file = os.path.join(index_wd, "{}.p".format(dict_name))
    pickle.dump( dict, open( pickle_file, "wb" ))

def cli_help_msg():
    msg ='''
    usage: python IndexEngine.py <path_to_latimes.gz> <path_to_index>
    '''
    print(msg)

if __name__ == '__main__':

    validate_args(sys.argv)
    print("Indexing the following data file: {} \n and storing index in: {}"\
            .format(sys.argv[1], sys.argv[2]))
    index_engine(sys.argv[1], sys.argv[2])
    print("Finished processing the file: {}".format(sys.argv[1]))
