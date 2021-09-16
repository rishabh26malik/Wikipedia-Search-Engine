## -----------------------------------------------------------------------------------------------------------------------
## 													IMPORTS
import sys
import bz2
import heapq
import os
import operator
from collections import defaultdict
import threading
import subprocess
import itertools
import re
import linecache
import pickle
import pprint

## -----------------------------------------------------------------------------------------------------------------------
##												GLOBAL VARIABLES
fileCount = 0
bodyFileCount = 0
infoFileCount = 0
categoryFileCount = 0
referenceFileCount = 0
extLinkFileCount = 0
titleFileCount = 0

bodyData = defaultdict(list)
infoData = defaultdict(list)
categoryData = defaultdict(list)
referenceData = defaultdict(list)
extLinkData = defaultdict(list)
titleData = defaultdict(list) 


# -----------------------------------------------------------------------------------------------------------------------
## 									WRITE TOKEN COUNT IN THE TEXT PARSED
def write_token_count(total_tokens, tokens_in_inverted_index, path_to_inverted_index):
	#filename = path_to_inverted_index + "/invertedindex_stat.txt"
	filename = "./invertedindex_stat.txt"
	with open(filename, 'wb') as f:
            f.write(string2Bytes(str(total_tokens) + '\n'))
            f.write(string2Bytes(str(tokens_in_inverted_index)))
	

## -----------------------------------------------------------------------------------------------------------------------
## 										CONVERT STRING TO BYTES DATATYPE
def string2Bytes(data):	## Convert string To bytes
	return data.encode('utf-8')
    
## -----------------------------------------------------------------------------------------------------------------------    
##								GETTING LIST OF LINE COUNT IN INTERMEDIATE INDEX FILES 
def getNumberOfLinesInSmallIndexFiles(fileCount, filePath):
	lines = []
	for i in range(0, fileCount):
		filename = filePath + 'index' + str(i) + '.txt'
		command = "cat " + filename + "| wc -l"
		lineCount = os.system(command)
		lines.append(lineCount)
	return lines
	
## -----------------------------------------------------------------------------------------------------------------------	          
## 											WRINTING FINAL INDEX FILES
def writeIndexFilesViaThreads(indexType, data, offset, countFinalFile, outputPath):
	ZipFilename = outputPath + indexType + str(countFinalFile) + '.bz2'
	TxtFilename = './inverted_indexes/' + indexType + str(countFinalFile) + '.txt'
	OffsetFilename = outputPath + indexType + '_offset' + str(countFinalFile) + '.txt'	
	print("writing ", len(data), TxtFilename)
	with open(TxtFilename, 'wb') as f:
            f.write(string2Bytes('\n'.join(data)))
	print("DONE writing ", TxtFilename)

## -----------------------------------------------------------------------------------------------------------------------
##										WRITING TITLES AND THEIR CORRESPONDING DOC IDS
def writeTitle_and_titleOffset_File(DOC_id):
	#filename = output_path+'title.txt'
	filename = './FinalIndices/'+'title.txt'
	with open(filename,'w') as f:
		f.write('\n'.join(data2write))

## -----------------------------------------------------------------------------------------------------------------------
##								WRITING/CREATING INTERMEDIATE INDEX FILES B4 CREATING FINAL INDEX FILES
def writeSmallIndexFiles( index, output_path, DOC_id, file_count, offset, path_to_inverted_index):
	data2write = []
	previousTitleOffset = offset
	filename = output_path + "index" + str(file_count) + ".txt.bz2"
	for word in sorted(index.keys()):
		tmp = str(word) + " " + ' '.join(index[word])
		data2write.append(tmp)

	with bz2.BZ2File(filename, 'w', compresslevel=9) as f:
		f.write(str.encode('\n'.join(data2write)))
    
	data2write=[]
	dataOffset=[]
	for key in DOC_id:
		data2write.append(str(key)+' '+DOC_id[key].decode('utf-8'))
		    
	filename = path_to_inverted_index + '/' +'title.txt'
	with open(filename,'a') as f:
		f.write('\n'.join(data2write))
	
	return  previousTitleOffset
	
## -----------------------------------------------------------------------------------------------------------------------
##												
def check_n_update_IMPvalue(Dict, DOC_Id, value, key, flag):
	if(value != '0.0'):
		Dict[key][DOC_Id] = float(value)
		flag = 1
	return flag

## -----------------------------------------------------------------------------------------------------------------------
def getSortedRowOfIndex(Dict, key):
	SortedRow = Dict[key]
	SortedRow = sorted(SortedRow, key = SortedRow.get, reverse=True)
	return SortedRow
	
## -----------------------------------------------------------------------------------------------------------------------
##								PREPARING FINAL INDEXES FOR WRITING IN FILE
def prepare_data_2_write_in_indexFiles(Dict, key, data2write, offset, previousItem):
	if key in Dict:
		tmp = key + " "
		sortedIndexFieldData = getSortedRowOfIndex(Dict, key)
		for doc in sortedIndexFieldData:
			tmp += doc + " " + str(Dict[key][doc]) + " "
		offsetValue = str(previousItem) + " " + str(len(sortedIndexFieldData))  
		offset.append(offsetValue)
		previousItem = len(tmp) + 1
		data2write.append(tmp)

## -----------------------------------------------------------------------------------------------------------------------
##	CHECK FINAL INDEX FILE SIZE. IF MORE THAN THRESHOLD THAN CREATE NEXT LEVEL INDEX FILES TO AVOID LARGE SIZED FILE
def checkFileSize():
	pathOfFolder = './inverted_indexes'
	global fileCount 

	bodySize = os.path.getsize(pathOfFolder+'/Body'+str(fileCount)+'.txt')
	infoSize = os.path.getsize(pathOfFolder+'/Info'+str(fileCount)+'.txt') 
	catgSize = os.path.getsize(pathOfFolder+'/Category'+str(fileCount)+'.txt')
	reffSize = os.path.getsize(pathOfFolder+'/References'+str(fileCount)+'.txt')
	linkSize = os.path.getsize(pathOfFolder+'/Link'+str(fileCount)+'.txt')
	titlSize = os.path.getsize(pathOfFolder+'/Title'+str(fileCount)+'.txt')
	maxFileSize = 1048576 * 500
	#threshold = bodySize*0.3 + infoSize*0.2 + catgSize*0.2 + reffSize*0.1 + linkSize*0.1 + titlSize*0.1
	threshold = bodySize + infoSize + catgSize + reffSize + linkSize + titlSize
	print("file size sum = ", threshold )
	#print(threshold)
	if ( threshold > maxFileSize ):
		print("File siz exceeded!!! Next level index files creating...")
		fileCount+=1

## -----------------------------------------------------------------------------------------------------------------------
## 				WRITING FINAL INDEX FILES USING THREAD,   SEPARATE THREAD FOR EACH FIELD TYPE FILE
class writeParallel(threading.Thread):                                                              
    
    def __init__(self, field, data, countFinalFile,pathOfFolder):
        threading.Thread.__init__(self)
        self.data=data
        self.field=field
        self.count=countFinalFile
        self.pathOfFolder=pathOfFolder
        
    def run(self):
        filename= './inverted_indexes/' + self.field + str(self.count) #self.pathOfFolder+'\\'+self.field+str(self.count)
        with open(filename+'.txt', 'ab') as f:
            f.write(str.encode('\n'.join(self.data)))
    
## -----------------------------------------------------------------------------------------------------------------------
##								MAIN CODE SECTION FOR WRITING FINAL INDEX FILES
def writeFinalIndex(data, countFinalFile, pathOfFolder,offsetSize):                                 
    title=defaultdict(dict)
    text=defaultdict(dict)
    info=defaultdict(dict)
    category=defaultdict(dict)
    externalLink=defaultdict(dict)
    references=defaultdict(dict)
    uniqueWords=[]
    offset=[]
    global fileCount
    print(len(data))
    print("preparing dictionaries")
    for key in sorted(data.keys()):
        listOfDoc=data[key]
        temp=[]
        flag=0
        for i in range(0,len(listOfDoc),7):
            word=listOfDoc
            docid=word[i]
            if word[i+1]!='0.0':
            	title[key][docid]=float(word[i+1])
            	flag=1

            if word[i+2]!='0.0':
                info[key][docid]=float(word[i+2])
                flag=1

            if word[i+3]!='0.0':
                category[key][docid]=float(word[i+3])
                flag=1

            if word[i+4]!='0.0':
                externalLink[key][docid]=float(word[i+4])
                flag=1

            if word[i+5]!='0.0':
                references[key][docid]=float(word[i+5])
                flag=1
                      
            if word[i+6]!='0.0':
                text[key][docid]=float(word[i+6])
                flag=1

        if flag==1:
            string = key+' '+str(countFinalFile)+' '+str(len(listOfDoc)/6)
            uniqueWords.append(string)
            offset.append(str(offsetSize))
            offsetSize=offsetSize+len(string)+1
    print("DONE preparing dictionaries")
    print("---",len(title), len(text), len(info), len(category), len(externalLink), len(references))
    titleData=[]
    textData=[]
    infoData=[]
    categoryData=[]
    externalLinkData=[]
    referencesData=[]

    print("preparing data to write")
    for key in sorted(data.keys()):                                                                     
        #print key
        ## --------------------------- TITLE INDEX PROCESSING ---------------------------
        if key in title:
            string=key+' '
            sortedField=title[key]
            sortedField = sorted(sortedField, key = sortedField.get, reverse=True)
            for doc in sortedField:
                string+=doc+' '+str(title[key][doc])+' '
            titleData.append(string)

        ## --------------------------- BODY INDEX PROCESSING --------------------------------
        if key in text:
            string=key+' '
            sortedField=text[key]
            sortedField = sorted(sortedField, key = sortedField.get, reverse=True)
            for doc in sortedField:
                string+=doc+' '+str(text[key][doc])+' '
            textData.append(string)       

        ## --------------------------- INFO INDEX PROCESSING --------------------------------
        if key in info:
            string=''
            string+=key+' '
            sortedField=info[key]
            sortedField = sorted(sortedField, key = sortedField.get, reverse=True)
            for doc in sortedField:
                string+=doc+' '+str(info[key][doc])+' '
            infoData.append(string)

        ## --------------------------- CATEGORY INDEX PROCESSING ----------------------------
        if key in category:
            string=key+' '
            sortedField=category[key]
            sortedField = sorted(sortedField, key = sortedField.get, reverse=True)
            for doc in sortedField:
                string+=(doc+' '+str(category[key][doc])+' ')
            categoryData.append(string)

        ## --------------------------- LINKS INDEX PROCESSING -------------------------------
        if key in externalLink:
            string= key+' '
            sortedField=externalLink[key]
            sortedField = sorted(sortedField, key = sortedField.get, reverse=True)
            for doc in sortedField:
                string+=doc+' '+str(externalLink[key][doc])+' '
            externalLinkData.append(string)

        ## --------------------------- REFERENCES INDEX PROCESSING ---------------------------
        if key in references:
            string= key+' '
            sortedField=references[key]
            sortedField = sorted(sortedField, key = sortedField.get, reverse=True)
            for doc in sortedField:
                string+=doc+' '+str(references[key][doc])+' '
            referencesData.append(string)
	
	## -----------------  THREADS FOR EACH OF 6 FIELD TYPE FILE WRITING -------------------------
    print("DONE preparing data to write")
    print(len(titleData), len(textData), len(infoData), len(categoryData), len(externalLinkData))        
    thread1 = writeParallel('Title', titleData,  fileCount,pathOfFolder)
    thread2 = writeParallel('Body', textData,  fileCount,pathOfFolder)
    thread3 = writeParallel('Info', infoData,  fileCount,pathOfFolder)
    thread4 = writeParallel('Category', categoryData,  fileCount,pathOfFolder)
    thread5 = writeParallel('Link', externalLinkData, fileCount,pathOfFolder)
    thread6 = writeParallel('References', referencesData, fileCount,pathOfFolder)

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    #checkFileSize()

    fileCount += 1
    ## -------------------DELETING INTERMEDIATE DATA STRUCTURES CREATED TO FREE UP MEMORY AND SPEED UP PROCESSING ------
    del titleData
    del textData
    del infoData
    del categoryData
    del externalLinkData
    del referencesData
    del references
    del externalLink
    del category
    del info
    del text
    del title
 
    #with open(pathOfFolder+"\\vocabularyList.txt","ab") as f:
    #  f.write('\n'.join(uniqueWords))
            
    return countFinalFile, offsetSize

## -----------------------------------------------------------------------------------------------------------------------
## 								CREATING SECONDARY INDEXES
def make_secondary_index():
	fileCount = 856
	fields = ['Body', 'Info', 'Title', 'Category', 'References', 'Link'] 
	secondary_index = defaultdict(dict)
	data = ''
	for field in fields:
		data += field
		for i in range(fileCount):
			filePath = './inverted_indexes/' + field + str(i) + '.txt'
			lineCount = int(subprocess.check_output('cat ' + filePath + ' | wc -l', shell=True, text=True).replace('\n',''))
			firstWord = linecache.getline(filePath, 1).strip().split()[0]
			lastWord = linecache.getline(filePath, lineCount).strip().split()[0]
			# Body 0 20000 aaa mmm 1 nnn rrr 15000 2 .... so on
			# field < fileNum lineCount firstWord lastWords > ....
			secondary_index[field][i] = {}
			secondary_index[field][i]['start'] = firstWord
			secondary_index[field][i]['end'] = lastWord
			secondary_index[field][i]['lineCount'] = lineCount
			data += ' ' + str(i) + ' ' + str(lineCount) + ' ' + firstWord + ' ' + lastWord + ' '	
		data += '\n'
	pprint.pprint(secondary_index)
	data = re.sub(' +', ' ', data)
	#with open('./inverted_indexes/secondary_index.txt','w') as f:
	#	f.write(data)
	with open('./inverted_indexes/secondary_index.txt','wb') as fp:
		pickle.dump(secondary_index, fp)

## -----------------------------------------------------------------------------------------------------------------------
## 						K - WAY MERGE ALGO TO MERGE SORTED INTERMEDIATE INDEX FILES TO MAKE FINAL INDEX FILES
def mergeFiles(pathOfFolder, countFile, path_to_inverted_index, total_tokens):                                                 
    listOfWords={}
    indexFile={}
    topOfFile={}
    flag=[0]*countFile
    data=defaultdict(list)
    data1=defaultdict(list)
    heap=[]
    countFinalFile=0
    offsetSize = 0
    global bodyData
    global infoData
    global categoryData
    global referenceData
    global extLinkData
    global titleData
    global fileCount 

    total_lines_file = [107858, 102068, 88920, 58550, 96766, 96809, 91782, 89079, 92165, 86458, 83141, 87385, 77463, 76545, 81470, 80626, 69820, 70650, 56917, 41889, 69123, 73711, 68528, 72711, 74414, 74392, 69510, 71350, 72571, 64472, 58637, 73748, 70374, 72733, 70832, 70930, 67939, 68973, 63888, 71277, 69190, 69809, 65653, 62935, 67464, 66573, 70735, 69553, 69510, 66338, 64816, 67017, 65147, 66948, 66302, 62368, 50787, 57711, 63443, 68195, 67675, 65172, 64518, 64934, 64791, 62074, 61373, 61792, 62057, 59723, 62494, 59505, 60272, 60365, 59998, 60236, 60280, 60318, 63139, 61009, 61272, 61567, 60401, 60967, 58643, 59626, 58644, 58019, 57700, 58117, 54487, 59967, 59030, 57235, 56586, 55307, 56322, 57029, 58339, 59446, 63649, 58782, 56848, 58523, 57240, 57462, 56923, 56506, 57114, 56773, 55965, 57312, 60136, 55340, 55857, 58582, 55660, 45071, 53977, 54756, 57183, 54476, 55652, 54902, 54761, 52840, 52180, 55752, 53895, 55517, 49242, 52510, 54236, 54232, 53745, 54013, 54833, 53341, 52322, 55621, 54318, 54008, 56616, 54086, 52482, 53619, 53647, 54784, 54294, 51798, 54674, 52359, 53777, 53347, 52714, 53325, 52186, 53526, 51838, 50899, 53156, 51919, 51259, 48801, 42763, 48931, 50670, 50027, 48923, 48776, 51005, 50301, 49328, 45200, 51206, 49004, 49113, 51958, 50557, 48534, 49833, 49631, 41785, 47911, 48871, 48985, 50172, 42825, 46890, 47167, 49897, 49144, 46260, 50077, 45072, 44983, 34406, 47710, 48548, 48344, 45546, 39519, 42206, 43282, 44632, 47465, 45796, 45822, 43347, 48551, 41980, 46447, 48039, 48679, 44273, 49485, 48879, 48701, 41281, 43120, 40707, 35194, 38824, 37388, 35925, 39045, 43776, 46711, 46309, 46862, 47293, 47453, 48123, 47174, 46658, 48059, 48608, 45949, 45372, 45377, 43660, 46656, 43900, 46783, 47347, 47791, 48110, 44755, 47921, 44678, 43964, 44641, 39319, 45714, 47451, 50369, 47166, 45427, 40655, 40734, 33513, 43938, 41746, 42681, 47150, 46755, 47118, 48097, 49416, 49538, 47326, 44870, 49450, 51382, 44175, 48775, 44949, 46141, 48834, 47305, 45090, 46286, 47878, 45917, 42945, 44112, 45043, 47453, 45331, 43299, 46762, 45274, 44991, 45909, 47501, 47792, 48077, 44810, 44978, 44269, 44977, 41553, 31906, 34565, 45676, 46929, 45202, 48026, 48320, 40977, 46327, 46711, 47245, 46267, 46400, 46854, 46617, 46604, 47892, 45569, 47434, 48884, 46909, 46875, 47627, 47665, 46410, 44593, 46423, 48367, 46305, 37574, 44794, 49092, 52161, 47147, 48322, 46996, 47499, 46916, 48134, 47658, 45799, 48127, 46157, 44965, 47244, 37179, 47310, 47262, 44446, 47932, 42540, 28232, 43413, 43145, 43609, 33844, 46526, 45814, 45782, 45217, 45469, 45885, 42307, 32953, 33853, 32570, 31114, 43127, 46521, 45757, 46132, 46574, 46692, 44105, 44879, 44606, 39030, 39874, 40742, 39063, 39300, 33601, 31680, 34864, 43975, 44091, 44604, 44792, 44681, 46188, 44929, 47428, 44165, 60088, 45025, 46663, 47233, 45012, 48235, 42689, 42912, 48820, 48384, 47045, 46918, 46952, 46509, 43661, 42097, 42078, 39204, 43895, 43894, 42164, 42168, 43996, 44570, 43134, 42043, 41816, 44797, 45782, 45455, 43379, 44279, 45990, 45190, 45721, 44342, 44713, 44459, 46500, 44102, 44109, 44699, 44398, 45447, 45480, 45017, 38215, 40019, 45144, 39887, 43421, 47516, 45877, 44523, 44433, 43721, 45536, 47888, 46938, 47353, 46286, 42215, 42933, 43727, 40533, 40171, 41738, 41270, 44152, 43817, 42019, 42967, 39167, 44158, 44411, 42315, 40299, 42360, 40635, 33700, 33151, 41371, 42777, 38652, 43967, 42685, 41140, 43095, 40699, 37022, 35255, 44189, 43396, 42101, 42952, 37681, 37326, 38465, 40566, 39519, 37773, 38961, 39882, 39881, 41608, 41478, 44107, 44126, 43558, 43284, 42695, 42786, 42913, 42053, 43072, 43340, 43189, 43247, 43969, 42740, 42394, 42227, 42474, 43178, 43414, 43554, 44593, 44011, 44179, 44156, 44358, 44251, 43044, 46149, 44578, 43516, 43441, 41792, 44080, 45318, 44983, 44579, 44209, 43167, 46174, 44845, 45093, 45517, 43214, 37603, 43449, 45060, 42647, 41710, 40641, 41563, 45603, 43761, 44278, 45109, 45251, 42378, 32842, 39651, 45413, 46226, 46099, 44958, 45571, 45247, 40984, 43737, 42268, 45653, 43253, 46059, 45728, 46004, 46122, 43607, 45362, 43834, 45818, 43873, 43199, 40831, 45001, 42633, 42841, 45419, 43223, 39817, 40852, 42194, 40970, 41431, 41164, 40204, 39844, 38616, 40234, 41712, 42767, 42444, 43368, 45109, 41303, 41469, 43557, 42739, 45759, 44480, 44242, 44319, 43319, 42100, 41800, 34397, 42090, 42959, 41683, 39618, 39639, 39039, 41573, 44302, 21494, 20628, 23716, 30465, 42980, 44366, 38145, 38099, 28404, 44932, 45642, 45429, 42257, 37616, 41717, 33578, 42330, 42147, 43571, 41007, 41052, 39177, 40770, 39433, 40558, 41787, 40885, 33643, 36332, 39020, 41365, 43789, 44062, 41861, 41386, 42199, 41081, 37646, 38899, 41201, 39282, 40469, 39753, 38894, 38044, 39216, 38484, 40578, 39092, 41198, 38264, 41849, 41359, 39924, 41457, 40587, 39572, 40924, 38450, 38454, 38383, 39547, 39356, 40432, 38292, 38911, 36457, 36004, 35629, 32200, 32331, 22397, 23796, 38578, 38586, 38631, 43141, 42513, 44124, 40115, 37873, 38581, 39596, 40270, 40461, 39474, 41385, 40145, 40630, 40161, 40447, 40731, 41458, 37364, 39330, 37428, 39035, 40817, 40022, 38808, 37854, 40516, 38700, 38140, 39272, 37390, 42105, 42781, 41672, 41509, 40880, 41619, 38169, 35401, 24388, 32077, 41002, 34027, 33529, 38798, 29552, 18562, 38012, 35986, 36426, 36877, 39642, 52830, 40559, 39574, 40716, 39723, 40388, 40678, 37132, 40474, 41330, 40261, 38645, 38808, 37165, 35927, 35233, 38183, 33820, 37908, 39594, 40746, 39375, 38521, 40334, 39788, 39741, 39876, 40003, 38633, 38997, 38497, 37927, 41162, 59797, 39584, 35161, 39373, 35235, 36124, 36377, 35551, 38148, 38534, 36571, 38579, 36807, 37071, 39113, 38943, 40416, 37799, 37140, 39989, 40512, 39179, 37921, 40411, 38610, 38545, 39928, 41273, 40014, 42226, 40315, 26119, 40171, 39043, 38036, 37997, 37225, 39980, 41387, 40226, 42461, 42179, 40517, 41909, 42121, 39400, 39709, 36497, 39634, 37096, 39129, 39034, 40225, 40316, 40650, 39119, 38874, 38517, 36888, 39119, 39774, 39942, 37155, 40025, 40323, 41381, 40798, 39206, 38925, 40217, 38660, 38339, 37364, 39064, 19972]
    print(sum(total_lines_file[:countFile]))
    print("Initializaing....")
    count_of_file = [0]*countFile
    for i in range(countFile):
        fileName = pathOfFolder + "index" + str(i) + ".txt.bz2"
        indexFile[i]= bz2.BZ2File(fileName, 'rb')
        flag[i]=1
        topOfFile[i]=indexFile[i].readline().strip().decode('utf-8')
        #topOfFile[i] = topOfFile[i].replace("  ", " ")
        topOfFile[i] = re.sub(r' +', ' ', topOfFile[i])
        listOfWords[i] = topOfFile[i].split(' ')
        if listOfWords[i][0] not in heap:
            heapq.heappush(heap, listOfWords[i][0])        
        count_of_file[i]+=1
    count=0        
    total_count = 0
    print("Initializatiopn DONE....")
    print(data)
    while any(flag)==1:
        temp = heapq.heappop(heap)
        if(temp==''):
        	print("aa")
        	continue
        	#break
        if (count % 25000 == 0):
	        print(count)
        count+=1
        total_count += 1
        count_next_letter = 0
        for i in range(countFile):
            if flag[i]:
            	
            	if listOfWords[i][0].lower() == temp.lower(): 
                    data[temp].extend(listOfWords[i][1:])
                    
                    if count == 150000:
                    	print(count, total_count)
                    	prevFileCount = fileCount
                    	print("IN for writing")
                    	countFinalFile, offsetSize = writeFinalIndex(data, fileCount, pathOfFolder, offsetSize)#, path_to_inverted_index)	
                    	print("BACK for writing")
                    	count = 0
                    	if prevFileCount != fileCount:
                    		print("Clearing data dict....")
                    		data = defaultdict(list)

                    topOfFile[i]=indexFile[i].readline().strip().decode('utf-8')
                    topOfFile[i] = re.sub(r' +', ' ', topOfFile[i])
                    count_of_file[i]+=1
                    if ( count_of_file[i] == total_lines_file[i] ):
                       	flag[i]=0
                       	print("File ",i," finished  ","Files left = ", flag.count(1))
                       	#os.remove(pathOfFolder+'index'+str(i)+'.txt.bz2')
                       	indexFile[i].close()
                    else:
                    	listOfWords[i] = topOfFile[i].split(' ')
                    	if listOfWords[i][0] not in heap:
                    		heapq.heappush(heap, listOfWords[i][0])
                    	
    print(count,"IN for writing....************")
    countFinalFile, offsetSize = writeFinalIndex(data, fileCount, pathOfFolder, offsetSize)#, path_to_inverted_index)
    print(count)
    print(flag[:countFile])
    print(count_of_file[:countFile])
    print(total_lines_file[:countFile])
    print( count_of_file[:countFile] == total_lines_file[:countFile] )
	
    make_secondary_index()
    tokens_in_inverted_index = int(subprocess.check_output('cat ' + path_to_inverted_index + '/vocabularyList.txt' + ' | wc -l', shell=True, text=True).replace('\n',''))
    write_token_count(total_tokens, tokens_in_inverted_index, path_to_inverted_index)




##   xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx END xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
