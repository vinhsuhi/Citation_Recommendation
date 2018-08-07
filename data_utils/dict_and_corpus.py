import logging
from nltk.corpus import stopwords 
from nltk import word_tokenize
import nltk
from library.utils import Pickle
from collections import Counter
import numpy as np
from gensim import corpora
import os, re
from gensim.models import Phrases





class PreprocessingData:
    """
    Tools to preprocess data
    Input: raw_corpus, list 
        A list of string, each string is equivalen to a document. 
    Output: list of bag of words

    Require: Remove stopwords, keep common bigram as a word, keep the important of 
    capitalize letter,...
    """


    def __init__(self, raw_corpus):
        self.raw_corpus = raw_corpus
        self.processed_corpus = None
        self.error = []
        self.preprocess_corpus()


    def remove_stopwords(self, words):
        """Remove stop words from list of tokenized words"""
        new_words = []
        for word in words:
            if word.lower() not in stopwords.words('english'):
                new_words.append(word)
        return new_words


    def compute_bigram(self, tokens):
        bigram = Phrases(tokens, min_count=20)
        for idx in range(len(tokens)):
            for token in bigram[tokens[idx]]:
                if '_' in token:
                    tokens[idx].append(token)
        return tokens 

    def impress_special_words(self, tokens, word_after_dot, keep):
        """
        special words should appear more than mean time that a word should appear
        """

        unspecial = [word for word in word_after_dot if word not in keep]
        new_tokens = []
        for token in tokens:
            if token in unspecial:
                new_tokens.append(token.lower())
            else:
                new_tokens.append(token)


        count = Counter(new_tokens)
        count_values = np.array(list(count.values()))
        try:
            special_words = []
            mean_time = int(count_values.mean())
            for word in list(count.keys()):
                if not word.islower():
                    if count[word] < mean_time + 1:
                        special_words.append(word)
            new_tokens += special_words*mean_time
        except Exception as err:
            print("Eror: ", new_tokens, err)
            new_tokens = None
        return new_tokens


    def preprocess_document(self, document):
        """
        input: document, string
        output: bag of word
        """
        word_after_dot = set([])
        keep = set([])
        tokens0 = word_tokenize(document)
        flag = 0
        for i, ele in enumerate(tokens0):
            if flag == 0 and ele.isalpha():
                flag = 1
                if not ele.islower():
                    word_after_dot.add(ele)
                continue
            if ele.isalpha() and not ele.islower():
                try:
                    if tokens0[i-1] == '.' and len(ele) > 2 and ele[1:].islower(): # remove Love but don't remove LOve
                        word_after_dot.add(ele)
                    else:
                        keep.add(ele)
                except:
                    continue


        tokens = [ele for ele in word_tokenize(document) if ele.isalpha() and len(ele) > 2]
        tokens2 = self.remove_stopwords(tokens)
        tokens3 = self.impress_special_words(tokens2, word_after_dot, keep)
        tokens4 = self.compute_bigram(tokens3)
        return tokens4


    def preprocess_corpus(self):
        self.processed_corpus = []
        for i, document in enumerate(self.raw_corpus):
            p = np.random.rand()
            if p < 0.001:
                print(i)
            tokens = self.preprocess_document(document)
            if tokens is None:
                self.error.append(i)
                print("hey: ", document, i)
                tokens = [ele for ele in word_tokenize(document) if ele.isalpha()]
            self.processed_corpus.append(tokens)


class DictAndCorpus:
    def __init__(self, corpus):
        self.corpus = corpus
        self.dict = None
        self.create_dictionary()

    def save_dictionay(self):
        self.dict.save('data/deerwester.dict')


    def save_corpus(self):
        corpus = [self.dict.doc2bow(doc) for doc in self.corpus]
        self.corpus = corpus
        corpora.MmCorpus.serialize('data/deerwester.mm', self.corpus)


    def create_dictionary(self):
        print("Creating dictionary!!!")
        dictionary = corpora.Dictionary(self.corpus)
        dictionary.filter_extremes(no_below=4, no_above=0.08, keep_n=None)
        self.dict = dictionary
        print("Saving dictionay...")
        self.save_dictionay()
        print("Saving corpus...")
        self.save_corpus()
        print("Done!")


class KeyWords:
    def __init__(self):
        self.keywords = None
        self.set_keywords = None
        self.key2doc = None
        self.preprocess_keywords()
        
    def load_data(self):
        self.keywords = Pickle.load_obj('data/key_words')
        self.set_keywords = Pickle.load_obj('data/set_keywords')

    def preprocess_keywords(self):
        self.load_data()
        key2doc = dict()
        i = 0 
        for kw in self.set_keywords:
            ids = []
            keyword_name = re.sub('\s', '', kw)
            for i, kw_of_doc in enumerate(self.keywords):
                if kw in kw_of_doc:
                    ids.append(i)
            if not key2doc.get(keyword_name):
                key2doc[keyword_name] = ids
                i += 1
        self.key2doc = key2doc
        Pickle.save_obj(self.key2doc, 'data/key2doc')


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', \
        level=logging.DEBUG, datefmt='%I:%M:%S')

    # preprocessing raw_copus
    if not os.path.exists('data/deerwester.dict'):
        if not os.path.exists('processed_data/prep_corpus.pkl'):
            print("Load data")
            raw_corpus = Pickle.load_obj('processed_data/raw_corpus')
            prep = PreprocessingData(raw_corpus).processed_corpus
            Pickle.save_obj(prep, 'processed_data/prep_corpus')
            raw_corpus = None
        else:
            prep = Pickle.load_obj('processed_data/prep_corpus')

        # create dictionay and corpus
        dict_and_corpus = DictAndCorpus(prep)
        dictionary = dict_and_corpus.dict
        corpus = dict_and_corpus.corpus
        prep = None
        keyword = KeyWords()
    else:
        dictionary = corpora.Dictionary.load('data/deerwester.dict')
        corpus = corpora.MmCorpus('data/deerwester.mm')

        print(len(corpus))
        print(corpus[0])
        keyword = Pickle.load_obj('data/key2doc')
        # print(keyword)
        max_len = 0

        # for key, value in keyword.items():
            # if len(value) > max_len:
                # max_len = len(value)
                # print(key)
        # print("Number of keyword: ", len(list(keyword.keys())))
        lenn = []
        count = 0
        for key, value in keyword.items():
            if len(value) > 150:
                count += 1
                lenn.append(len(value))
            print(len(value))
        print(np.mean(lenn))
        print(count)
        # bad_ids = []
        # for i, doc in enumerate(corpus):
        #     if len(doc) < 5:
        #         bad_ids.append(i)
        #         print(doc)
        # Pickle.save_obj(bad_ids, 'processed_data/bad_ids')
        # print(len(bad_ids))

    # dictionary and corpus
    







# def remove_non_ascii(words):
#     """Remove non-ASCII characters from list of tokenized words"""
#     new_words = []
#     for word in words:
#         new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
#         new_words.append(new_word)
#     return new_words

# def to_lowercase(words):
#     """Convert all characters to lowercase from list of tokenized words"""
#     new_words = []
#     for word in words:
#         new_word = word.lower()
#         new_words.append(new_word)
#     return new_words

# def remove_punctuation(words):
#     """Remove punctuation from list of tokenized words"""
#     new_words = []
#     for word in words:
#         new_word = re.sub(r'[^\w\s]', '', word)
#         if new_word != '':
#             new_words.append(new_word)
#     return new_words

# def replace_numbers(words):
#     """Replace all interger occurrences in list of tokenized words with textual representation"""
#     p = inflect.engine()
#     new_words = []
#     for word in words:
#         if word.isdigit():
#             new_word = p.number_to_words(word)
#             new_words.append(new_word)
#         else:
#             new_words.append(word)
#     return new_words



# def stem_words(words):
#     """Stem words in list of tokenized words"""
#     stemmer = LancasterStemmer()
#     stems = []
#     for word in words:
#         stem = stemmer.stem(word)
#         stems.append(stem)
#     return stems

# def lemmatize_verbs(words):
#     """Lemmatize verbs in list of tokenized words"""
#     lemmatizer = WordNetLemmatizer()
#     lemmas = []
#     for word in words:
#         lemma = lemmatizer.lemmatize(word, pos='v')
#         lemmas.append(lemma)
#     return lemmas

# def normalize(words):
#     words = remove_non_ascii(words)
#     words = to_lowercase(words)
#     words = remove_punctuation(words)
#     words = replace_numbers(words)
#     words = remove_stopwords(words)
#     return words

# words = normalize(words)
# print(words)