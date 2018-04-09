"""Get Doc.
Author: Nikhil Arora (NikhilJArora@outlook.com)

This module is intended to retrieve a document and its metadata from the dir
built by IndexEngine.py by using the

Examples
--------
The following examples is how the program can be used
literal blocks::
GetDoc /home/smucker/latimes-index docno LA010189-0018
    $ python GetDoc.py <index_wd> <key: ('docno' or 'docid')> <value>
    $ python GetDoc.py /Users/nikhilarora/data/latimes/index_dir_test docno LA010189-0014



Sample Output
-------------
docno: LA010189-0018
internal id: 76235
date: January 1, 1989
headline: OUTTAKES: MATERIAL MOLL
raw document:
<DOC>
...
</DOC>
"""
import os
import sys
import pickle
import gzip
from index_helpers import MetaData


def validate_args(args):
    """Ensures that requirements are met by the passed args"""
    if len(args) != 4:
        print("ERROR: incorrect length of args.")
        cli_help_msg()
        sys.exit()

    #first argument: check it is a read directory
    if args[1].lower().strip() == 'docid' or args[1].lower().strip() == 'docno':
        print("ERROR: incorrect key.  Please pass 'docid' or 'docno'")
        sys.exit()


def GetDoc(index_wd, key, value):
    """Grabs doc and metadata corresponding to key, value pair.

    Parameters
    ----------
    index_wd : str
        path to index that will be searched
    key : str
        can be either 'docno' or 'docid'
    value : int or str
        lookup value corresponding to spec'd key

    Returns
    -------
    bool :
        True if found and False if not found
    """
    # init code:
    key = key.lower().strip()
    value = value.strip()
    docid_to_docno_path = os.path.join(index_wd, 'docid_to_docno.p')
    docno_to_data_path = os.path.join(index_wd, 'docno_to_data.p')
    # check to see what key is (docno or docid)

    # if docid, need to grab the docid_to_docno from disk
    # Lookup via docid:
    if key == 'docid':
        docid_to_docno = pickle.load(open(docid_to_docno_path, "rb"))
        value = int(value)
        if value not in docid_to_docno:
            print("The following docid: {} was not found.".format(value))
            return False
        else:
            docno = docid_to_docno[value]
    #Lookup via docno:
    elif key == 'docno':
        docno = value
    else:
        print("The following key: {} is invalid.  Pass: 'docid' or 'docno'".format(key))
        return False
    docno_to_data = pickle.load(open(docno_to_data_path, "rb"))

    if docno not in docno_to_data:
        print("The following docno: {} was not found.".format(value))
        return False

    doc_path = docno_to_data[docno]

    basepath = os.path.dirname(doc_path)

    if not os.path.exists(basepath):
        print("The basepath {} does not exist.".format(basepath))
        return False

    metadata = MetaData(doc_path)
    metadata.load()
    metadata.meta_print()
    return True

def cli_help_msg():
    msg ='''
    usage: python GetDoc.py index_location docno LAMMDDYY-NNNN
           python GetDoc.py index_location docid N
    '''
    print(msg)

if __name__ == '__main__':
    validate_args(sys.argv)
    GetDoc(sys.argv[1], sys.argv[2], sys.argv[3])
