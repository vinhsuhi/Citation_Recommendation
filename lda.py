from gensim.test.utils import common_corpus
from gensim import corpora

from gensim.models import AuthorTopicModel
from library.utils import Pickle
# print(common_corpus)
# common_corpus is a list of 10 very short document

import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def main():
    author2doc = Pickle.load_obj('data/key2doc')
    print("done loading author")
    dictionary = corpora.Dictionary.load('data/deerwester.dict')
    print("done loading dictionary")
    token2id = dictionary.token2id
    id2token = {y:x for x,y in token2id.items()}
    print("done create id2 token")
    corpus = corpora.MmCorpus('data/deerwester.mm')
    print("done loading corpus")
    print("Start inferencing")
    model = AuthorTopicModel(corpus=corpus, num_topics=150, id2word=id2token, \
                author2doc=author2doc, chunksize=1500, passes=50, eval_every=0, \
                iterations=200, random_state=1, serialized=True, serialization_path='data/x.mm')
    print("done inferrence")
    model.save('data/model.atmodel')
    print("done save model")
    # chunks is batchs 
    # passes is epochs
if __name__ == "__main__":
    main()
