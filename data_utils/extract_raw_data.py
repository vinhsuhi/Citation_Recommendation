import json
from library.utils import MongoDB, Pickle
import numpy as np
import os
from copy import deepcopy
import random
import time
def read_file(path):
    """
    read file s2-corpus-00
    each paper is represented as a dictionary wich keys is:
    1.id  string
    S2 generated research paper ID.

    2.title  string
    Research paper title.

    3.paperAbstract  string
    Extracted abstract of the paper.

    4.entities  list
    S2 extracted list of relevant entities or topics.

    5.s2Url  string (not use)
    URL to S2 research paper details page.

    6.s2PdfUrl  string (not use)
    URL to PDF on S2 if available.

    7.pdfUrls  list (not use)
    URLs related to this PDF scraped from the web.

    8.authors  list
    List of authors with an S2 generated author ID and name.

    9.inCitations  list
    List of S2 paperId's which cited this paper.

    10.outCitations  list
    List of paperId's which this paper cited.

    11.year  int
    Year this paper was published as integer.

    12.venue  string (not use)
    Extracted venue published.

    13.journalName  string (not use)
    Name of the journal that published this paper.

    14.journalVolume  string (not use)
    The volume of the journal where this paper was published.

    15.journalPages  string (not use)
    The pages of the journal where this paper was published.

    16.sources  list (not use)
    Identifies papers sourced from DBLP or Medline.

    17.doi  string (not use)
    Digital Object Identifier registered at doi.org.

    18.doiUrl  string (not use)
    DOI link for registered objects.

    19.pmid  string (not use)
    Unique identifier used by PubMed.


    we just use some of those information...

    So we will construct new datas structure to save what we need, the data structure is as below
    Only papers publicted in 2017 are kept
    """
    # if os.path.exists('processed_data/abstract.json'):
        # return 0
    papers = MongoDB.get_collection(database_name='OpenCorpus', collection_name='papers')
    n_papers = papers.count()
    if n_papers > 300000:
        print("Data has already in DB, number of papers is: ", n_papers) # 26673
        return papers

    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        count = n_papers
        for i, line in enumerate(f):
            paper_info = json.loads(line)
            try:
                needed_info = {
                    "id": paper_info['id'],                     # "4cd223df721b722b1c40689caa52932a41fcc223"
                    "title": paper_info['title'],               # "Knowledge-rich, computer-assisted composition of Chinese couplets"
                    "abstract": paper_info['paperAbstract'],    # "Recent research effort in poem composition has focused on the use of automatic language generation..."
                    "entities": paper_info['entities'],         # ["Conformance testing","Natural language generation"]
                    "authors": paper_info['authors'],           # [{"name": "Vinh", "ids":["123456"]},...{}...]
                    "incitations": paper_info['inCitations'],   # [ids..]
                    'outcitations': paper_info['outCitations'], # [ids...]
                    "year": paper_info['year']
                }
            except Exception as err:
                continue
            if 2015 <= needed_info['year']  and needed_info['id'] != '' and needed_info['title'] != '' and needed_info['abstract'] != ''\
                            and needed_info['authors'] != [] and needed_info['entities'] != []:
                if needed_info['incitations'] != [] or needed_info['outcitations'] != []:
                    count += 1
                    MongoDB.insert_documents(database_name='OpenCorpus', collection_name='papers', doc=needed_info)
                    a = np.random.rand(1)
                    if a < 0.001:
                        print(count)
        print("Number of papers is: ", count)
        return papers

    # return data

def save_pkl(papers):
    """
    input: papers: a collection of papers
    output: a collection of [ids, incitations, outcitations]
            just consider inside our collection. Papers have both empty incitations and outcitations will be mark as isolate
            and will be remove from collection.
    """
    cite_map = MongoDB.get_collection(database_name='OpenCorpus', collection_name='cite_map')
    n_cite = cite_map.count()
    if n_cite > 10000:
        print("Data has already in DB, number of cite is: ", n_cite)
        return cite_map
    ids = []
    cursor = papers.find(no_cursor_timeout=True)
    for i, paper in enumerate(cursor):
        ids.append(paper['id'])
    cursor.close()
    Pickle.save_obj(ids, 'processed_data/ids')

    # citation is list of id in ids and in either incitations or outcitations
    print(type(ids))
    citations = []
    citations_time = []
    for i, id_ in enumerate(ids):
        start_time = time.time()
        paper = papers.find_one({'id': id_})
        
        a = [citation for citation in paper['incitations'] if citation not in citations and citation in ids]
        citations += a
        b = [citation for citation in paper['outcitations'] if citation not in citations and citation in ids]
        citations += b


        citations_time.append(time.time() - start_time)
        print("citations time", time.time() - start_time)
        print(i, len(citations))


    Pickle.save_obj(citations, 'processed_data/citations')

    count = 0
    cites = []
    for i, id_ in enumerate(citations):
        paper = papers.find_one({'id': id_})
        try:
            incitations = paper['incitations']
            filt_incitations = [incitations[i] for i in range(len(incitations)) if incitations[i] in citations]
            
            outcitations = paper['outcitations']
            filt_outcitations = [outcitations[i] for i in range(len(outcitations)) if outcitations[i] in citations]
        except Exception as err:
            print(err)
            continue

        cite = {'id': id_, 'incitations': filt_incitations, 'outcitations': filt_outcitations}
        count += 1
        print(count, i)
        print(cite)
        MongoDB.insert_documents(database_name='OpenCorpus', collection_name='cite_map', doc=cite)
        cites.append(cite)
    Pickle.save_obj(cites, 'processed_data/cite_map')
    return MongoDB.get_collection(database_name='OpenCorpus', collection_name='cite_map')


def filter(cite_map):
    final_cite_map = MongoDB.get_collection(database_name='OpenCorpus', collection_name='final_cite_map')
    n_cite = final_cite_map.count()
    if n_cite > 10000:
        print("Data has been in DB, number of cite is: ", n_cite)
        return final_cite_map
    new_cite_map = MongoDB.get_collection(database_name='OpenCorpus', collection_name='new_cite_map')
    n_cite = new_cite_map.count()
    if n_cite > 10000:
        print("Data has been in DB, number of cite is: ", n_cite)
    cursor = cite_map.find(no_cursor_timeout=True)
    if os.path.exists('processed_data/new_ids.pkl'):
        new_ids = Pickle.load_obj('processed_data/new_ids')
    else:
        ids = []
        for i, paper in enumerate(cursor):
            ids.append(paper['id'])
        cursor.close()
        Pickle.save_obj(ids, 'processed_data/ids')
        new_ids = []
        count = 0
        for i, id_ in enumerate(ids):
            node = cite_map.find_one({'id': id_})
            if len(node['incitations']) + len(node['outcitations']) > 1:
                count += 1
                print(count, i, len(ids))
                new_ids.append(id_)
        Pickle.save_obj(new_ids, 'processed_data/new_ids')

    final_ids = new_ids
    iterr = 0
    for k in range(8):
        iterr += 1
        ids = []
        count = 0
        for i, id_ in enumerate(final_ids):
            cite = cite_map.find_one({'id': id_})
            start_time = time.time()
            try:
                incitations = cite['incitations']
                filt_incitations = [incitations[i] for i in range(len(incitations)) if incitations[i] in final_ids]
                
                outcitations = cite['outcitations']
                filt_outcitations = [outcitations[i] for i in range(len(outcitations)) if outcitations[i] in final_ids]
            except Exception as err:
                print(err)
                continue

            if len(filt_incitations) > 1 or len(filt_outcitations) > 1:
                ids.append(id_)
                cite = {'id': id_, 'incitations': filt_incitations, 'outcitations': filt_outcitations}
                count += 1
                print(count, i, len(final_ids), time.time() - start_time)
                MongoDB.insert_documents(database_name='OpenCorpus', collection_name='new_cite_map' + str(iterr), doc=cite)

        new_cite_map = MongoDB.get_collection(database_name='OpenCorpus', collection_name='new_cite_map' + str(iterr))
        Pickle.save_obj(ids, 'processed_data/final_ids')
        count = 0
        final_ids = []
        for i, id_ in enumerate(ids):
            cite = new_cite_map.find_one({'id': id_})
            start_time = time.time()
            try:
                incitations = cite['incitations']
                filt_incitations = [incitations[i] for i in range(len(incitations)) if incitations[i] in ids]
                
                outcitations = cite['outcitations']
                filt_outcitations = [outcitations[i] for i in range(len(outcitations)) if outcitations[i] in ids]
            except Exception as err:
                print(err)
                continue

            if len(filt_incitations) > 0 or len(filt_outcitations) > 0:
                final_ids.append(id_)
                cite = {'id': id_, 'incitations': filt_incitations, 'outcitations': filt_outcitations}
                count += 1
                print(count, i, len(ids), time.time() - start_time)
                MongoDB.insert_documents(database_name='OpenCorpus', collection_name='final_cite_map' + '2' + str(iterr), doc=cite)
        Pickle.save_obj(final_ids, 'processed_data/final_ids')
        cite_map = MongoDB.get_collection(database_name='OpenCorpus', collection_name='final_cite_map' + '2' + str(iterr))

def collect_paper(final_cite_map):
    ids = Pickle.load_obj('processed_data/final_ids')
    citations = []
    for i, id_ in enumerate(ids):
        cite = final_cite_map.find_one({'id': id_})
        citations.append({'incitations': cite['incitations'], 'outcitations': cite['outcitations']})
        print(i, len(citations[i]['incitations']), len(citations[i]['outcitations']))
    papers = MongoDB.get_collection(database_name='OpenCorpus', collection_name='papers')
    for i, id_ in enumerate(ids):
        paper_info = papers.find_one({'id': id_})
        needed_info = {
                    "id": paper_info['id'],                     # "4cd223df721b722b1c40689caa52932a41fcc223"
                    "title": paper_info['title'],               # "Knowledge-rich, computer-assisted composition of Chinese couplets"
                    "abstract": paper_info['abstract'], # "Recent research effort in poem composition has focused on the use of automatic language generation..."
                    "entities": paper_info['entities'],         # ["Conformance testing","Natural language generation"]
                    "authors": paper_info['authors'],           # [{"name": "Vinh", "ids":["123456"]},...{}...]
                    "incitations": citations[i]['incitations'], # [ids..]
                    'outcitations': citations[i]['outcitations'],   # [ids...]
                    "year": paper_info['year']
                }
        print("adding new info to collection")
        MongoDB.insert_documents(database_name='OpenCorpus', collection_name='final_papers2', doc=needed_info)
        a = np.random.rand(1)
        if a < 0.1:
            print(i, len(ids))

def save_text_to_file():
    papers = MongoDB.get_collection(database_name='OpenCorpus', collection_name='final_papers2')
    ids = Pickle.load_obj('processed_data/final_ids')
    raw_corpus = []
    for i, id_ in enumerate(ids):
        paper = papers.find_one({'id': id_})
        raw_corpus.append(paper['abstract'])
        p = random.random()
        if p < 0.001:
            print(i)
    Pickle.save_obj(raw_corpus, 'processed_data/raw_corpus')

def remove_bad_id_papers(bad_idx):
    papers = MongoDB.get_collection(database_name='OpenCorpus', collection_name='final_papers')
    ids = Pickle.load_obj('processed_data/final_ids')
    bad_ids = [ids[i] for i in bad_idx]
    good_ids = [ele for ele in ids if ele not in bad_ids]
    final_ids = []
    flag = 0
    for id_ in good_ids:
        paper = papers.find_one({'id': id_})
        incitations = paper['incitations']
        new_incitation = [ele for ele in incitations if ele not in bad_ids]
        outcitations = paper['outcitations']
        new_outcitation = [ele for ele in outcitations if ele not in bad_ids]
        if len(new_incitation) > 0 or len(new_outcitation) > 0:
            final_ids.append(id_)
            cite = {'id': id_, 'incitations': new_incitation, 'outcitations': new_outcitation}
            MongoDB.insert_documents(database_name='OpenCorpus', collection_name='final_cite_map_over', doc=cite)
        else:
            print("flag now is 1")
            flag = 1
    if flag == 0:
        Pickle.save_obj(final_ids, 'processed_data/final_ids')
        final_cite_map = MongoDB.get_collection(database_name='OpenCorpus', collection_name='final_cite_map_over')
        collect_paper(final_cite_map)
        save_text_to_file()

    else:
        print("HUHUHUHUHU")

def get_keywords():
    papers = MongoDB.get_collection(database_name='OpenCorpus', collection_name='final_papers2')
    ids = Pickle.load_obj('processed_data/final_ids')
    set_keywords = set([])
    key_words = []
    years = []
    titles = []
    ids2idx = {}
    for i, id_ in enumerate(ids):
        ids2idx[id_] = i
        paper = papers.find_one({'id':id_})
        key_words.append(paper['entities'])
        years.append(paper['year'])
        titles.append(paper['title'])
        set_keywords.update(paper['entities'])
        p = random.random()
        if p < 0.001:
            print(i, key_words[i])
    Pickle.save_obj(key_words, 'data/key_words')
    Pickle.save_obj(years, 'data/years')
    Pickle.save_obj(key_words, 'data/key_words')
    Pickle.save_obj(titles, 'data/titles')
    Pickle.save_obj(set_keywords, 'data/set_keywords')


if __name__ == "__main__":
    # path = "raw_data"
    # load data to mongodb
    # papers = None
    # dirr = os.listdir(path)
    # 00 - > 06
    # while len(dirr) > 1:
    #   for ele in dirr:
    #       if not ele.endswith('.txt'):
    #           new_path = path + '/' + ele
    #           print(new_path)
    #           papers = read_file(new_path)
    #           os.remove(new_path)
    #           break
    #   dirr = os.listdir(path)
    # papers = read_file(path)
    # print("start mapping")
    # maps = save_pkl(papers)
    # final_cite_map = filter(maps)
    # ids = Pickle.load_obj('processed_data/final_ids')
    # print(len(ids))
    # collect_paper(final_cite_map)
    # save_text_to_file()
    # bad_ids = Pickle.load_obj('processed_data/bad_ids')
    # remove_bad_id_papers(bad_ids)
    # get_keywords()
    author2doc = Pickle.load_obj('data/key2doc')
    count = 0
    # print(author2doc.keys())
    for keys, values in author2doc.items():
        if count < 100:
            print(values)
        count += len(values)
        # print(len(values))

    print(count) # 518434



