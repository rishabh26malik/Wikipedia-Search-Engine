from collections import defaultdict
import threading
import sys
import bz2
import re
import math
from nltk.stem import PorterStemmer
import nltk
from nltk.stem import WordNetLemmatizer
offset=[]
import os
import subprocess
import linecache
from textProcessing import MyTextProcessing
import json
import datetime
import timeit
import pickle
import random
from pprint import pprint

path_to_inverted_index = ""


class WikiSearch():
	def __init__(self, indexPath):
		self.textProcess = MyTextProcessing()
		global path_to_inverted_index
		path_to_inverted_index = indexPath
		self.vocabPath = path_to_inverted_index + '/vocabularyList.txt'
		self.titlePath = path_to_inverted_index + '/Title'
		self.categoryPath = path_to_inverted_index + '/Category'
		self.infoPath = path_to_inverted_index + '/Info'
		self.referencesPath = path_to_inverted_index + '/References'
		self.externalLinksPath = path_to_inverted_index + '/Link'
		self.bodyPath = path_to_inverted_index + '/Body'#0.txt'
		self.queryTokens = []
		self.vocabLineCount = int(subprocess.check_output('cat ' + self.vocabPath + ' | wc -l', shell=True, text=True).replace('\n',''))
		self.results = []
		self.documentFrequency = {}
		self.commonTitleDocs = []
		self.commonInfoDocs = []
		self.commonCategoryDocs = []
		self.commonReferencesDocs = []
		self.commonExternalLinksDocs = []
		self.commonBodyDocs = []
		self.secondaryIndex = {}
		self.fileCount = 17
	
	# -------------------------------------  LOAD SECONDARY INDEX B4 ENTERING QUERIES  -----------------------------------
	def loadSecondaryindex(self):
		filePath = './inverted_indexes/secondary_index.txt'
		with open(filePath, 'rb') as fp:
			self.secondaryIndex = pickle.load( fp)
		#pprint(self.secondaryIndex)
		
	# -------------------------------------  CUSTOM PAGE RANKING  --------------------------------------------------------		
	def pageRanking(self, t="plain" ):
		rankedResults = {'Title':[], 'Body':[], 'Info':[], 'Category':[], 'Link':[], 'References':[]}
		fields = ['Title', 'Body', 'Info', 'Category', 'Link', 'References']
		if ( t == "multi"):
			for field in fields:
				temp = self.results[field]
				for word in temp.keys():
					rankedResults[field] += self.results[field][word][::2][:5]
				random.shuffle(rankedResults[field])
		
		else:
			for word in self.results.keys():
				temp = self.results[word]
				for field in fields:
					rankedResults[field] += self.results[word][field][::2][:5]
				random.shuffle(rankedResults[field])
		
		finalResults = []
		countResults = 0
#		for field in ['Title', 'Info', 'Category', 'Body',  'Link', 'References']:
		if ( len(rankedResults['Title']) >= 3 ):
			finalResults += rankedResults['Title'][:4]
		
		countResults = 10 - len(finalResults)
		
		if ( len(rankedResults['Info']) >= 2 ):
			finalResults += rankedResults['Info'][:3]
		
		countResults = 10 - len(finalResults)
		
		if ( len(rankedResults['Category']) >= 2 ):
			finalResults += rankedResults['Category'][:3]
		
		countResults = 10 - len(finalResults)
		
		if ( len(rankedResults['Body']) >= 2 ):
			finalResults += rankedResults['Body'][:3]
		
		countResults = 10 - len(finalResults)
		
		if ( len(rankedResults['Link']) >= 1 ):
			finalResults += rankedResults['Link'][:2]
		
		countResults = 10 - len(finalResults)
		
		if ( len(rankedResults['References']) >= 1 ):
			finalResults += rankedResults['References'][:2]
		finalResults = finalResults[:10]
		countResults = 10 - len(finalResults)
		finalResults = list(map(int, finalResults))
		#print("finalResults",finalResults)
		return finalResults
		listOfDocuments=defaultdict(float)            
		return listOfDocuments
		
	# --------------------------------------  BINARY SEARCH TO FIND WORD IN A FILE --------------------------------------
	def findInIndexFile(self, low, high, indexFilePath, word):
		while(low <= high):
			mid = low + (high-low)//2  
			line = linecache.getline(indexFilePath, mid)
			line = line.split()
			if not line:
				return [], -1
			if(line[0] == word):
				return line[1:], mid
			elif (word > line[0]):
				low = mid + 1
			else:
				high = mid - 1
		return [], -1
	
	# ------------------------------------- SEARCH WORD IN VOCAB FIRST, IF EXIST THEN SEARCH IN INDICES -----------------	
	def checkInVocab(self, word):
		low = 0
		high = self.vocabLineCount
		while( low <= high ):
			mid = low + (high - low)//2
			testWord = linecache.getline(self.vocabPath, mid)
			if (isinstance(testWord, bytes)):
				testWord = testWord.decode('utf-8')
			testWord = testWord.strip().split(' ')
			if word==testWord[0]:
				return testWord[1:], mid, True
			elif word>testWord[0]:
				low=mid+1
			else:
				high=mid-1
		return [], -1, False
	
	# --------------------------------------  MULTI FIELD QUERY PROCESSING TO GET EACH FIELD QUEY SEAPRATELY -------------
	# Basic processing based on input syntax to extract query terms along with their field types
	def processMultiFieldQuery(self, query):
		queryText = re.split('[t|i|c|b|l|r]:', query)
		queryText = list(filter(('').__ne__, queryText))
		fieldType = re.findall('[t|c|i|b|l|r]:',query)
		num_fields = len(fieldType)
		queries = {}
		fieldTypes = []
		for i in range(num_fields):
			q = queryText[i].strip()
			if ( fieldType[i][0] == 't'):
				fieldTypes.append('t')
				queries['t'] = self.textProcess.basicProcessing(q, True)
			elif ( fieldType[i][0] == 'c'):
				fieldTypes.append('c')
				queries['c'] = self.textProcess.basicProcessing(q, True)
			elif ( fieldType[i][0] == 'i'):
				fieldTypes.append('i')
				queries['i'] = self.textProcess.basicProcessing(q, True)
			elif ( fieldType[i][0] == 'r'):
				fieldTypes.append('r')
				queries['r'] = self.textProcess.basicProcessing(q, True)
			elif ( fieldType[i][0] == 'l'):
				fieldTypes.append('l')
				queries['l'] = self.textProcess.basicProcessing(q, True)
			elif ( fieldType[i][0] == 'b'):
				fieldTypes.append('b')
				queries['b'] = self.textProcess.basicProcessing(q, True)
		return queries, fieldTypes
	
	# --------------------------------------  GETTING COMMON DOCIDS AMONGST GIVEN DOCIDS---------------------------------
	def getCommonDocs(self, data, indexType):
		commonDOC_Ids = {}			
		#print("-->>>  ",indexType, data.keys())	
		#print(data.keys())
		for key in data.keys():
			word = key
			commonDOC_Ids = set(data[key][::2])
			break
		#print(commonDOC_Ids)
		for key in data.keys():	
			docIds = set(data[key][::2])
			#print("\n",key, docIds,"\n")
			if (key !=word and docIds != set()):
				#print(key)
				#print(data[key])
				commonDOC_Ids = commonDOC_Ids & docIds
		commonDOC_Ids = list(map(int, commonDOC_Ids))
		#print("--- ",commonDOC_Ids)
		if (indexType == 'Title'):
			self.commonTitleDocs = commonDOC_Ids
		elif (indexType == 'Category'):
			self.commonCategoryDocs = commonDOC_Ids
		elif (indexType == 'Info'):
			self.commonInfoDocs = commonDOC_Ids
		elif (indexType == 'References'):
			self.commonReferencesDocs = commonDOC_Ids
		elif (indexType == 'Link'):
			self.commonExternalLinksDocs = commonDOC_Ids
		elif (indexType == 'Body'):
			self.commonBodyDocs = commonDOC_Ids
		
		#return commonDOC_Ids

	# ----------------------------------------  MULTI FIELD QUERY RESOLVER  --------------------------------------------
	def multiFieldQueryResolver(self, query):
		fileList = defaultdict(dict)
		query, fieldTypes = self.processMultiFieldQuery(query)
		self.queryTokens = [q for fieldQry in list(query.values()) for q in fieldQry]
		titleIndexData = {}
		infoIndexData = {}
		categoryIndexData = {}
		referencesIndexData = {}
		externalLinksIndexData = {}
		bodyIndexData = {}
		fieldMap = {'t':'Title', 'i':'Info', 'c':'Category', 'b':'Body', 'r':'References', 'l':'Link'}
		for fieldType in fieldTypes:
			fieldQueries = query[fieldType]
			if( fieldType == 't' ):
				for token in fieldQueries:
					fileNum, lineCount = self.checkWhichIndexFileToLook('Title', token)
					if (fileNum != -1):
						obtainedList, _ = self.findInIndexFile(0, lineCount, self.titlePath + str(fileNum) + '.txt', token)
					else:
						obtainedList, mid = [], -1
					titleIndexData[token] = obtainedList
		
				fileList[fieldMap[fieldType]] = titleIndexData	
					
			elif( fieldType == 'i' ):
				for token in fieldQueries:
					fileNum, lineCount = self.checkWhichIndexFileToLook('Info', token)
					if (fileNum != -1):
						obtainedList, _ = self.findInIndexFile(0, lineCount, self.infoPath + str(fileNum) + '.txt', token)
					else:
						obtainedList, mid = [], -1
					infoIndexData[token] = obtainedList #[0::2]
					
				fileList[fieldMap[fieldType]] = infoIndexData	
					
			elif( fieldType == 'c' ):
				for token in fieldQueries:
					fileNum, lineCount = self.checkWhichIndexFileToLook('Category', token)
					if (fileNum != -1):
						obtainedList, _ = self.findInIndexFile(0, lineCount, self.categoryPath + str(fileNum) + '.txt', token)
					else:
						obtainedList, mid = [], -1
					categoryIndexData[token] = obtainedList
				fileList[fieldMap[fieldType]] = categoryIndexData	
				
			elif( fieldType == 'r' ):
				for token in fieldQueries:
					fileNum, lineCount = self.checkWhichIndexFileToLook('References', token)
					if (fileNum != -1):
						obtainedList, _ = self.findInIndexFile(0, lineCount, self.referencesPath + str(fileNum) + '.txt', token)
					else:
						obtainedList, mid = [], -1
					referencesIndexData[token] = obtainedList 
				fileList[fieldMap[fieldType]] = referencesIndexData	
				
			elif( fieldType == 'l' ):
				for token in fieldQueries:
					fileNum, lineCount = self.checkWhichIndexFileToLook('Link', token)
					if (fileNum != -1):
						obtainedList, _ = self.findInIndexFile(0, lineCount, self.externalLinksPath + str(fileNum) + '.txt', token)
					else:
						obtainedList, mid = [], -1
					externalLinksIndexData[token] = obtainedList
				fileList[fieldMap[fieldType]] = externalLinksIndexData	
			elif( fieldType == 'b' ):
				for token in fieldQueries:
					fileNum, lineCount = self.checkWhichIndexFileToLook('Body', token)
					if (fileNum != -1):
						obtainedList, _ = self.findInIndexFile(0, lineCount, self.bodyPath + str(fileNum) + '.txt', token)
					else:
						obtainedList, mid = [], -1
					bodyIndexData[token] = obtainedList
				fileList[fieldMap[fieldType]] = bodyIndexData
			#fileList[token][fieldMap[fieldType]] = obtainedList		

		return fileList
	
	# --------------------  GET PARTICULAR INDEXFILE WHICH MIGHT CONTAIN DETAILS OF INPUT QUERY WORD----------------------
	def checkWhichIndexFileToLook(self, field, word):
		for i in range(self.fileCount):
			if self.secondaryIndex[field][i]['start'] <= word and self.secondaryIndex[field][i]['end'] >= word :
				return i, self.secondaryIndex[field][i]['lineCount']
		return -1, -1
	
	# -------------------------------------- PLAIN(FIELD TYPE NOT SPECIFIED) QUERY RESOLVER--------------------------------
	def plainQueryResolver(self):
		fileList = defaultdict(dict)
		docFreq = {}
		indexTypes = ['Title', 'Info', 'Category', 'References', 'Link', 'Body'] 
		for token in self.queryTokens:
			#print("\n----------------Vocab-------------------\n")
			vocabTokenData, position, isPresent = self.checkInVocab(token)
			#print("vocab returnedList - ",vocabTokenData)
			if ( isPresent == True ):
				fileNumber = vocabTokenData[0]
				docFreq[token] = vocabTokenData[1]
				for idxType in indexTypes:
					if ( idxType == 'Title' ):
						#print("\n----------------Title-------------------\n")
						fileNum, lineCount = self.checkWhichIndexFileToLook(idxType, token)
						if (fileNum != -1):
							obtainedList, mid = self.findInIndexFile(0, lineCount, self.titlePath + str(fileNum) + '.txt', token)
						else:
							obtainedList, mid = [], -1
						
					elif ( idxType == 'Info' ):
						#print("\n----------------Info-------------------\n")
						fileNum, lineCount = self.checkWhichIndexFileToLook(idxType, token)
						if (fileNum != -1):
							obtainedList, mid = self.findInIndexFile(0, lineCount, self.infoPath + str(fileNum) + '.txt', token)
						else:
							obtainedList, mid = [], -1
						
					elif ( idxType == 'Category' ):
						#print("\n----------------Category-------------------\n")
						fileNum, lineCount = self.checkWhichIndexFileToLook(idxType, token)
						if (fileNum != -1):
							obtainedList, mid = self.findInIndexFile(0, lineCount, self.categoryPath + str(fileNum) + '.txt', token)
						else:
							obtainedList, mid = [], -1
						
					elif ( idxType == 'References' ):
						#print("\n----------------References-------------------\n")
						fileNum, lineCount = self.checkWhichIndexFileToLook(idxType, token)
						if (fileNum != -1):
							obtainedList, mid = self.findInIndexFile(0, lineCount, self.referencesPath + str(fileNum) + '.txt', token)
						else:
							obtainedList, mid = [], -1
						
					elif ( idxType == 'Link' ):
						#print("\n----------------Links-------------------\n")
						fileNum, lineCount = self.checkWhichIndexFileToLook(idxType, token)
						if (fileNum != -1):
							obtainedList, mid = self.findInIndexFile(0, lineCount, self.externalLinksPath + str(fileNum) + '.txt', token)
						else:
							obtainedList, mid = [], -1
						
					elif ( idxType == 'Body' ):
						#print("\n----------------Body-------------------\n")
						fileNum, lineCount = self.checkWhichIndexFileToLook(idxType, token)
						if (fileNum != -1):
							obtainedList, mid = self.findInIndexFile(0, lineCount, self.bodyPath + str(fileNum) + '.txt', token)
						else:
							obtainedList, mid = [], -1
					
					fileList[token][idxType] = obtainedList
		return fileList	, docFreq
	
	# --------------------------------------------------------------------------------------------------------------------
	def getOutputDOC_Ids(self, words):
		output = defaultdict(dict)
		for word in words:
			output[word] = {'Title':[], 'Info':[], 'Category':[], 'References':[], 'Link':[], 'Body':[]}
		for word in words:
			for x in ['Title', 'Info', 'Category', 'References', 'Link', 'Body']:
				try:
					output[word][x] = list(map(int, self.results[word][x][::2]))
					if(len(output[word][x]) > 3):
						output[word][x] = output[word][x][:3]
				except:
					output[word][x] = []
				if not output[word][x]:
					 output[word][x].append('NOT FOUND')
				
		jsonData = json.dumps(output, indent = 2)
		return jsonData 
			
	# --------------------------------------------------------------------------------------------------------------------
	#def getTitles1(self, allResults):
	#	if (isinstance(allResults, dict)):
	#		DOC_Ids_allResults = list(allResults.keys())
	#	else:
	#		DOC_Ids_allResults = allResults
	#	resultCount = 10 if len(DOC_Ids_allResults) >= 10 else len(DOC_Ids_allResults)
	#	top10resultsDOC_id = DOC_Ids_allResults[:resultCount]
	#	titleResults = []
	#	titleFileName = './inverted_indexes/title.txt'
	#	for DOC_id in top10resultsDOC_id:
	#		doc_title = linecache.getline(titleFileName, int(DOC_id))#.split()#[1]
	#		titleResults.append(doc_title)
	#	return titleResults
		
	# ------------------------------------  GET DOCUMENT TITLES FOR DOCIDS  ----------------------------------------------
	def getTitles(self, allResults):
		if (isinstance(allResults, dict)):
			DOC_Ids_allResults = list(allResults.keys())
		else:
			DOC_Ids_allResults = allResults
		resultCount = 10 if len(DOC_Ids_allResults) >= 10 else len(DOC_Ids_allResults)
		top10resultsDOC_id = DOC_Ids_allResults[:resultCount]
		titleResults = []
		titleFileName = './inverted_indexes/title.txt'
		for DOC_id in top10resultsDOC_id:
			doc_title = linecache.getline(titleFileName, int(DOC_id)+1)#.split()#[1]
			titleResults.append(doc_title)
		return titleResults
	
	# ------------------------------------- QUERY RESOLVER STARTER CODE SECTION ------------------------------------------
	def resolveQuery(self, query):  #----------->> main()
		if (re.search(r'[t|r|c|l|i|b]:',query)):
			self.results = self.multiFieldQueryResolver(query)
			#print(self.results)
			rankedResults = self.pageRanking(t="multi")
		else:
			self.queryTokens = self.textProcess.basicProcessing(query, True)
			self.results, self.documentFrequency = self.plainQueryResolver()
			rankedResults = self.pageRanking(t="plain")
		jsonData = self.getOutputDOC_Ids(self.queryTokens)
		#print("results ",rankedResults)
		if ( len(rankedResults) == 0 ):
			#print("NOT FOUND")
			with open("./queries_op.txt", 'a') as out:
				out.write("NOT FOUND\n")
			return
		titleResults = self.getTitles(rankedResults)
		output_results = ""
		for docid, docTitle in zip(rankedResults, titleResults):
			output_results += docTitle
		with open("./queries_op.txt", 'a') as out:
			out.write(output_results)
		#for x in titleResults:
		#    print(x)	
	
	# --------------------------------------------- MAIN -----------------------------------------------------------------------	
if __name__ == "__main__":                                            
	if (len(sys.argv) !=2 ):
		print(len(sys.argv), "Invalid number of arguments!!!")
		sys.exit(0)
	search = WikiSearch(sys.argv[1])
	search.loadSecondaryindex()
	with open("./queries.txt") as file:
		queries = file.readlines()
	for query in queries:
		start = timeit.default_timer()
		query = query.lower()
		query = query.strip()
		search.resolveQuery(query)
		stop = timeit.default_timer()
		total_seconds = math.ceil(stop - start)
		with open("./queries_op.txt", 'a') as out:
			out.write(str(stop - start)+'\n\n')
		#print("Total time taken = ",str(datetime.timedelta(seconds=total_seconds)))
