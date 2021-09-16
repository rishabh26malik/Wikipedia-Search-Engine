import xml.sax.handler                                                  
#import xml.sax.handler.ContentHandler as contentHandler
from collections import defaultdict
import sys
import timeit
import threading
import re                                                           
#from Stemmer import Stemmer
from nltk.stem import PorterStemmer
import nltk
from nltk.stem import WordNetLemmatizer
from MyfileHandling import writeSmallIndexFiles, mergeFiles
from itertools import chain
#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## ***************************************  TEXT PROCESSING CLASS  *******************************************************

class MyTextProcessing():

	def __init__(self):
		self.categoryPattern = r'\[\[category:(.*?)\]\]'
		self.infoboxPattern ="{{Infobox((.|\n)*?)}}"
		self.pattern = re.compile("[^a-zA-Z0-9]")
		self.externalLinksPattern = r'==External links==\n[\s\S]*?\n\n'
		self.referencesPattern = r'== ?references ?==(.*?)\n\n' #r'==References==\n[\s\S]*?\n\n'
		self.removeLinksPattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',re.DOTALL)
		self.removeSymbolsPattern = r"[~`!@#$%-^*+{\[}\]\|\\<>/?]"
		self.stopwords = self.getStopWords()
		self.infoBox_dict = {}
		self.category_dict = {}
		self.external_links_dict = {}
		self.references_dict = {}
		self.body_dict = {}
	
	## ------------------ LEMMATIZATION -----------------------------------------------------------------------
	def stemming(self, data):											
		#nltk.download('wordnet')
		stemmedWords = []
		lemmatizer=WordNetLemmatizer()	
		for word in data:
			stemmedWords.append(lemmatizer.lemmatize(word))	
		return stemmedWords	
	
	## ------------------ STOP WORDS REMOVAL ----------------------------------------------------------------
	def removeStopWords(self, data):										## Removal of stop words to get only meaningful words
		stopWords_removed = [word for word in data if self.stopwords[word]!=1]
		return stopWords_removed
  		
  	## ------------------ TOKENIZATION -----------------------------------------------------------------------
	def tokenize(self, data):
		'''
		tokens=re.findall("\d+|[\w]+",data)
		tokens=[ self.string2Bytes(key) for key in tokens]
		'''
		tokens = data.split()
		return tokens
	
	## ------------------ STRING TO BYTES CONVERSION --------------------------------------------------------
	def string2Bytes(self, data):	## Convert string To bytes
		return data.encode('utf-8')
	
	## ------------------ BYTES TO STRING CONVERSION --------------------------------------------------------
	def bytes2String(self, data):	## Convert bytes to string
		return data.decode(utf-8)
	
	## ------------------ MAKING DICTIONARY OF COUNT OF EACH WORD IN INPU TTEXT -----------------------------
	def makeDictionary(self, words):
		tmp_dict = defaultdict(int)
		for word in words:
			if (len(word)>2):
				OnlyEnglisgPattern = re.compile("^[a-zA-Z]+$")
				if OnlyEnglisgPattern.match(word) is not None:
					tmp_dict[word]+=1
		return tmp_dict
	
	## ------------------ TOKENIZATION --> STOP WORD REMOVAL --> LEMMATIZE --> MAKE COUNT DICTIONARY -------
	def basicProcessing(self, data, flagForSearch = False):
		#data = data.lower()
		tokens = self.tokenize(data)
		stopWords_removed = self.removeStopWords(tokens)
		stemmed_words = self.stemming(stopWords_removed)
		if(flagForSearch == False):
			dictionary = self.makeDictionary(stemmed_words)
			return dictionary
		else:
			return stemmed_words	
	
	## ------------------ READING STOP WORDS FROM A SET OF STOPWORDS FROM A FILE -----------------------------
	def getStopWords(self):										## Reading stopwords to be used for processing
		stopwords = defaultdict(int)
		with open('stopwords.txt','r') as f:
			for line in f:
				stopwords[line.strip()]=1
		return stopwords
	
	## ------------------ MAKING COUNT DICTIONARY OF TITLES -------------------------------------------------
	def processTitle(self, data):
		#print("TITLE - ", data)
		data = data.lower()
		processedTitle = self.basicProcessing(data)
		return processedTitle	
	
	## ------------------ INFOBOX ---------------------------------------------------------------------------
	def processInfoBoxData(self, data):
		infobox_data = []
		info_box_reg_exp = r'{{infobox(.*?)}}\n'			#r'{{infobox((.|\n)*?)}}'
		infobox = re.findall(r'{{infobox((.|\n)*?)}}', data, flags=re.IGNORECASE)
		if(len(infobox)<=15):
			#print(len(infobox))
			infobox = list(chain(*infobox))
			#print(len(infobox), infobox)
			for line in infobox:
				tokens = re.findall(r'=(.*?)\|',line,re.DOTALL)
				#print(tokens)
				infobox_data.extend(tokens)
		infobox_data = ' '.join(infobox_data)
		infobox_data = re.sub(self.removeSymbolsPattern, ' ', infobox_data)
		self.infoBox_dict = self.basicProcessing(infobox_data)

	## ------------------ CATEGORY ---------------------------------------------------------------------------
	def processCategoryData(self, data):
		categories = re.findall(self.categoryPattern, data, flags=re.MULTILINE | re.IGNORECASE)
		categories = ' '.join(categories)
		#categories = self.removeSymbolsPattern.sub(' ', categories)
		self.category_dict = self.basicProcessing(categories)
	
	## ------------------ EXTERNAL LINKS -----------------------------------------------------------------------		
	def processExternalLinksData(self, data):
		links = re.findall(self.externalLinksPattern, data, flags= re.IGNORECASE)
		links = " ".join(links)
		links = links[20:]
		links = re.sub('[|]', ' ', links)
		links = re.sub('[^a-zA-Z ]', ' ', links)
		self.external_links_dict = self.basicProcessing(links)

	## ------------------ BODY --------------------------------------------------------------------------------	
	def processBodyData(self, data):
		body = data
		body = re.findall(r'== ?[a-z]+ ?==\n(.*?)\n', body)
		body = " ".join(body)
		body = re.sub(self.removeSymbolsPattern, " ", body)
		self.body_dict = self.basicProcessing(body)

	## ------------------ REFERENCES -----------------------------------------------------------------------
	def processReferences(self, data):
		references = re.findall(self.referencesPattern, data, flags=re.DOTALL | re.MULTILINE | re.IGNORECASE)
		references = ' '.join(references)
		references = re.sub(self.removeSymbolsPattern, ' ', references)
		self.references_dict = self.basicProcessing(references)
		
	## ------------------ REMOVING USELESS TEXT -------------------------------------------------------------
	def removeUnnessaryDetails(self, data):
		data = data.lower()
		data = re.sub(self.removeLinksPattern, ' ', data)#, flags = re.DOTALL)
		data = re.sub(r'{\|(.*?)\|}', ' ', data, flags = re.DOTALL)
		data = re.sub(r'{{v?cite(.*?)}}', ' ', data, flags = re.DOTALL)
		data = re.sub(r'<(.*?)>', ' ', data, flags = re.DOTALL)
		return data
	
	## ------------------ INFOBOX ---------------------------------------------------------------------------		
	def processData(self, data, titleFlag, textFlag):
		if(titleFlag==True):
			return self.processTitle(data)
		elif(textFlag == True):
			data = self.removeUnnessaryDetails(data)			
			self.processCategoryData(data)
			self.processReferences(data)
			self.processInfoBoxData(data)
			self.processExternalLinksData(data)
			self.processBodyData(data)

			return self.infoBox_dict, self.category_dict, self.external_links_dict, self.references_dict, self.body_dict
#-----------------------------------------------------------------------------------------------------------------------

