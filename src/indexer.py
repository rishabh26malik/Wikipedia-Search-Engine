
# **************************  MODULES   **************************

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
from textProcessing import MyTextProcessing
import os
import math
import datetime
#-----------------------------------------------------------------------------------------------------------------------

# **************************   GLOBAL VARIABLES **************************

index=defaultdict(list)
stopwords = defaultdict(int)		
DOC_id = {}
count = 0
file_count = 0
offset = 0
path_to_inverted_index = ""
total_tokens = 0
totalDocsCount = 0

#-----------------------------------------------------------------------------------------------------------------------

class IndexCreator():
	def getIMPvalue(self, dictionary, data_value, size, count):
		tmp = ""
		try:
			if data_value in dictionary.keys():
				tmp += str(round(dictionary[data_value]/size, 3)) + " "
			else:
				tmp += "0.0 "
		except ZeroDivisionError:
			tmp += "0.0 "
		return tmp
		
	def makeIndices(self, title, infoBox, category, external_links, references, body):
		global count
		global file_count
		global offset
		global index
		global DOC_id
		global path_to_inverted_index
		links_count = float(len(external_links))
		title_count = float(len(title))
		infoBox_count = float(len(infoBox))
		category_count = float(len(category))
		references_count = float(len(references))
		body_count = float(len(body))
		vocab = list(set( list(title.keys()) + list(infoBox.keys()) + list(category.keys()) + list(external_links.keys()) + list(references.keys()) + list(body.keys()) ) )
		#if ( len(external_links)>1 ):
		#	print(external_links)
		#	vocab = list(set( vocab + list(external_links.keys()) ) )
		
		vocab.sort()
		#print(len(vocab), vocab)
		# word DocID title info category link ref body .... 
		for word in vocab:
			tmp = str(count) + " "
			tmp += self.getIMPvalue(title, word, title_count, count)
			tmp += self.getIMPvalue(infoBox, word, infoBox_count, count)
			tmp += self.getIMPvalue(category, word, category_count, count)
			tmp += self.getIMPvalue(external_links, word, links_count, count)
			tmp += self.getIMPvalue(references, word, references_count, count)
			tmp += self.getIMPvalue(body, word, body_count, count)
			index[word].append(tmp)
			#print(word, tmp)
		count += 1
		if(count%25000==0):
			print(count)
			output_path = "./tmp/"
			offset = writeSmallIndexFiles(index, output_path, DOC_id, file_count, offset, path_to_inverted_index)
			index = defaultdict(list)
			DOC_id = {}				
			file_count += 1
			#print("offset = ",offset)

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#       ************************************************************    XML PARSING CLASS    ************************************************************						
				
class WikiHandler(xml.sax.handler.ContentHandler):
	
	def __init__(self):
		self.titleFlag = False
		self.idFlag = False
		self.textFlag = False
		self.pageStartFlag = False
		self.text = ""
		self.title = ""
		self.id = ""
		self.textProcess = MyTextProcessing()
	
	
	def startElement(self, name, attribute):
		if (name == "id"):
			self.idFlag = True
			self.pageStartFlag = True
			self.id = ""
		elif (name == "title"):
			self.titleFlag = True
			self.title = ""
			
		elif (name == "text"):
			self.textFlag = True
			self.text = ""


			
	def endElement(self, name):
		global total_tokens
		global totalDocsCount
		if (name == "id"):
			self.idFlag = False
		elif (name == "title"):
			WikiHandler.titleWords = self.textProcess.processData(self.title, True, False)
			self.titleFlag = False
			#print("TITLE - ",WikiHandler.titleWords)
		elif (name == "text"):
			WikiHandler.infoBox_data, WikiHandler.category_data, WikiHandler.externalLinks_data, WikiHandler.references_data, WikiHandler.body_data = self.textProcess.processData(self.text, False, True)
			#print("INFOBOX - ",WikiHandler.infoBox_data)
			#print("CATEGORY - ",WikiHandler.category_data)
			#print(DOC_id)
			index = IndexCreator()
			index.makeIndices(WikiHandler.titleWords, WikiHandler.infoBox_data, WikiHandler.category_data, WikiHandler.externalLinks_data, WikiHandler.references_data, WikiHandler.body_data)
			total_tokens += sum(list(WikiHandler.titleWords.values()))
			total_tokens += sum(list(WikiHandler.infoBox_data.values()))
			total_tokens += sum(list(WikiHandler.category_data.values()))
			total_tokens += sum(list(WikiHandler.externalLinks_data.values()))
			total_tokens += sum(list(WikiHandler.references_data.values()))
			total_tokens += sum(list(WikiHandler.body_data.values()))
			#if(totalDocsCount == 20000):
			#	sys.exit(0)
		elif (name == "page"):
			self.pageStartFlag = False
			totalDocsCount += 1
			#print("--------------page End-----------------\n--------------------------\n")

	
	
	def characters(self, data):
		global count
		global DOC_id
		if (self.idFlag):
			self.id += data
		elif (self.titleFlag):
			DOC_id[count] = self.textProcess.string2Bytes(data)    ##xXXXXXXXXXXXXXx
			self.title += data
		elif (self.textFlag):
			
			self.text += data
		
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def main():
	global file_count
	global offset
	global DOC_id
	global index
	global path_to_inverted_index
	global total_tokens
	global totalDocsCount
	if (len(sys.argv) != 3):
		print("Invalid number of arguments!!!")
		sys.exit(0)
	inputFile = sys.argv[1]
	path_to_inverted_index = sys.argv[2]
	try:
		os.mkdir('./results')
		os.mkdir('./inverted_indexes')
		#os.mkdir('./inverted_indexes/2020201074')
		os.mkdir('./tmp')
		os.mkdir(path_to_inverted_index)
	except:
		pass
	start = timeit.default_timer()
	#inputFile = "/home/rishabh/Downloads/data"
	#inputFile = "enwiki-latest-pages-articles17.xml-p23570393p23716197"
   	#-----------------------#SAX Parser-----------------------	
	'''
	parser = xml.sax.make_parser()                                  
	handler = WikiHandler()
	parser.setContentHandler(handler)
	parser.parse(inputFile)
	#---------------------------------------------------------
	offset = writeSmallIndexFiles(index, "./tmp/", DOC_id, file_count, offset, path_to_inverted_index)
	file_count+=1
	
	#mergeFiles('./tmp/', file_count, path_to_inverted_index, total_tokens)
    '''
	#mergeFiles('/media/rishabh/New Volume/PHASE-2/tmp/', 856, path_to_inverted_index, 10000)
	stop = timeit.default_timer()
	sec = math.ceil(stop - start)
	print("Total time taken = ",str(datetime.timedelta(seconds=sec)))
	print("total Docs Count - ", totalDocsCount)
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":                                           
	main()