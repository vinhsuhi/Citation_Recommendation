from gensim.test.utils import common_corpus
from gensim import corpora
import os
from gensim.models import AuthorTopicModel
from library.utils import Pickle
# print(common_corpus)
# common_corpus is a list of 10 very short document

import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def create_doc2author(corpus, author2doc):
    doc2author = {}
    for d, _ in enumerate(corpus):
        author_ids = []
        for a, a_doc_ids in author2doc.items():
            if d in a_doc_ids:
                author_ids.append(a)
        doc2author[d] = author_ids
    return doc2author

def main():
    corpus = corpora.MmCorpus('data/deerwester.mm')
    author2doc = Pickle.load_obj('data/key2doc')
    if os.path.exists('data/doc2author.pkl'):
        logger.info("Loading doc2author")
        doc2author = Pickle.load_obj('data/doc2author')
    else:
        doc2author = create_doc2author(corpus, author2doc)
        Pickle.save_obj(doc2author, 'data/doc2author')
    dictionary = corpora.Dictionary.load('data/deerwester.dict')
    token2id = dictionary.token2id
    id2token = {y:x for x,y in token2id.items()}
    model = AuthorTopicModel(corpus=corpus, num_topics=150, id2word=id2token, \
                author2doc=author2doc, doc2author=doc2author, chunksize=1500, \
                passes=50, eval_every=0, \
                iterations=200, random_state=1, serialized=True,\
                serialization_path='data/x.mm')
    print("done inferrence")
    model.save('data/model.atmodel')
    print("done save model")
    # chunks is batchs 
    # passes is epochs
if __name__ == "__main__":
    main()
