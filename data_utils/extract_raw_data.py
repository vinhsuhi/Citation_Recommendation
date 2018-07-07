import json
from library.utils import MongoDB
import numpy as np

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
	with open(path, 'r', encoding='utf-8', errors='ignore') as f:
		papers = MongoDB.get_collection(database_name='OpenCorpus', collection_name='papers')
		n_papers = papers.count()
		if n_papers > 30000:
			print("Data has already in DB, number of papers is: ", n_papers) # 26673
			return papers
		count = 0
		for i, line in enumerate(f):
			paper_info = json.loads(line)
			try:
				needed_info = {
					"id": paper_info['id'],						# "4cd223df721b722b1c40689caa52932a41fcc223"
					"title": paper_info['title'],				# "Knowledge-rich, computer-assisted composition of Chinese couplets"
					"abstract": paper_info['paperAbstract'],	# "Recent research effort in poem composition has focused on the use of automatic language generation..."
					"entities": paper_info['entities'],			# ["Conformance testing","Natural language generation"]
					"authors": paper_info['authors'],			# [{"name": "Vinh", "ids":["123456"]},...{}...]
					"incitations": paper_info['inCitations'],	# [ids..]
					'outcitations': paper_info['outCitations'],	# [ids...]
					"year": paper_info['year']
				}
			except Exception as err:
				continue
			if 2016 <= needed_info['year']  and needed_info['id'] != '' and needed_info['title'] != '' and needed_info['abstract'] != ''\
							and needed_info['authors'] != [] and needed_info['entities'] != []:
				if needed_info['incitations'] != [] or needed_info['outcitations'] != []:
					count += 1
					MongoDB.insert_documents(database_name='OpenCorpus', collection_name='papers', doc=needed_info)
					a = np.random.rand(1)
					if a < 0.01:
						print(count)
		print("Number of papers is: ", count)
		return papers

	# return data

def create_map(papers):
	"""
	input: papers: a collection of papers
	output: a collection of [ids, incitations, outcitations]
			just consider inside our collection. Papers have both empty incitations and outcitations will be mark as isolate
			and will be remove from collection.
	"""
	maps = MongoDB.get_collection(database_name='OpenCorpus', collection_name='cite_map')
	if maps.count() > 100:
		print("Data has already in DB, number of papers is ", maps.count())
		return maps

	for paper in papers.find():
		cite_info = {}
		outcitations = paper['outcitations']
		try:
			filted_outcitations = []
			for citation in outcitations:
				cited_paper = papers.find_one({'id': citation})
				if cited_paper is not None:
					filted_outcitations.append(cited_paper['id'])
		except:
			continue
		try:	
			incitations = paper['incitations']
			filted_incitations = []
			for citation in incitations:
				citing_paper = papers.find_one({'id': citation})
				if citing_paper is not None:
					filted_incitations.append(citing_paper['id'])
		except:
			continue
		# remove papers that don't have link to any paper
		try:
			if filted_incitations != [] or filted_outcitations != []:
				cite_info['id'] = paper['id']
				cite_info['outcitations'] = filted_outcitations
				cite_info['incitations'] = filted_incitations
				print(cite_info)
				MongoDB.insert_documents(database_name='OpenCorpus', collection_name='cite_map', doc=cite_info)
		except:
			continue	
	return maps


if __name__ == "__main__":
	path = "raw_data/s2-corpus-00"
	papers = read_file(path)
	create_map(papers)
