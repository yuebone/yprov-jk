import os
import json

def _readJson(dirPath,fileName,length):

    if length<0:
        return False,'error','the length of file '+fileName+'not recorded in stat.json'

    filePath=os.path.join(dirPath,fileName)
    if not os.path.exists(filePath):
        return False,'error','file '+filePath+'doesnt exist'

    fileF=open(filePath,'rt')
    readStr=fileF.read(length)
    fileF.close()
    readLen=len(readStr)
    if readLen<length:
        return False,'error','the length of file '+fileName+\
        ' is '+readLen+',which at least should be '+length

    j=json.loads(readStr)

    return True,j,None

def yprovReadJson(dirPath):

    #check whether files exist
    statPath=os.path.join(dirPath,'stat.json')
    if not os.path.exists(statPath):
        return False,'warnning','file '+statPath+'doesnt exist'

    relMapPath=os.path.join(dirPath,'relmap.json')
    if not os.path.exists(relMapPath):
        return False,'error','file '+relMapPath+'doesnt exist'

    #read stat.json
    statFile=open(statPath,'rt')
    statJson=json.load(statFile)
    statFile.close()
    relLen=int(statJson.get('rel',-1))
    tRelLen=int(statJson.get('trel',-1))
    exprLen=int(statJson.get('expr',-1))
    cExprLen=int(statJson.get('cexpr',-1))

    #read relmap.json
    relMapFile=open(relMapPath,'rt')
    relMapJson=json.load(relMapFile)
    relMapFile.close()

    #read other json files
    isOk,statusOrJson,msg=_readJson(dirPath,'rel.json',relLen)
    if not isOk:
        return isOk,status,msg
    relJson=statusOrJson

    isOk,statusOrJson,msg=_readJson(dirPath,'trel.json',tRelLen)
    if not isOk:
        return isOk,status,msg
    tRelJson=statusOrJson

    isOk,statusOrJson,msg=_readJson(dirPath,'expr.json',exprLen)
    if not isOk:
        return isOk,status,msg
    exprJson=statusOrJson

    isOk,statusOrJson,msg=_readJson(dirPath,'cexpr.json',cExprLen)
    if not isOk:
        return isOk,status,msg
    cExprJson=statusOrJson

    return True,statJson,(relMapJson,relJson,tRelJson,exprJson,cExprJson)


def simpleChk(dirPath):
    isOk,status,msg=yprovReadJson(dirPath)
    if isOk:
        print('it is ok by checking simply')
    else:
        print(status,':',msg)

def _genDotTable(filex,id,head,trs,headColor,trColor):
    lineStr='{}\t[shape=none label=<\n'\
    '\t<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">\n'\
    .format(id)
    filex.write(lineStr)

    lineStr='\t<TR><TD PORT="at" BGCOLOR="{}">{}</TD></TR>\n'.format(headColor,head)
    filex.write(lineStr)

    i=0
    for tr in trs:
        lineStr='\t<TR><TD PORT="at{}" BGCOLOR="{}">{}</TD></TR>\n'\
        .format(i,trColor,tr)
        i+=1
        filex.write(lineStr)

    filex.write('\t</TABLE>\n>];\n\n')

def _escapeDotStr(rawStr):
    newStr=rawStr.replace('<','''\<''')
    newStr=newStr.replace('>','''\>''')
    newStr=newStr.replace('|','''\|''')
    return newStr

class Dot():
    def __init__(self,dirPath,relJ,trelJ,exprJ,cexprJ):
        self.relJ=relJ
        self.trelJ=trelJ
        self.exprJ=exprJ
        self.cexprJ=cexprJ
        self.dirPath=dirPath


    def relToDot(self,name,tblId):
        tblJ=self.relJ.get(tblId)
        if not tblJ:
            return

        _tblFilePath=os.path.join(self.dirPath,name+'_tbl.dot')
        _colFilePath=os.path.join(self.dirPath,name+'_col.dot')
        self.tblFile=open(_tblFilePath,'wt')
        self.colFile=open(_colFilePath,'wt')
        self.tblSaved=set()
        self.tblFile.write('/*generated @ y_prov*/\n\ndigraph G {\nrankdir=BT;\n\n')
        self.colFile.write('/*generated @ y_prov*/\n\ndigraph G {\nrankdir=RL;\n\n')

        self._tblToDot(tblId)

        self.tblFile.write('\n}\n')
        self.colFile.write('\n}\n')
        self.tblFile.close()
        self.colFile.close()


    def _tblFromTo(self,f,t):
        if f and t:
            self.tblFile.write(f+'\t->\t'+t+'\t;\n\n')

    def _colFromTo(self,f,t):
        if f and t:
            self.colFile.write(f+'\t->\t'+t+'\t;\n\n')


    def _tblToDot(self,id):
        if not id[0]:
            return None
        if id[0]=='r':
            return self.__relToDot(id,self.relJ.get(id))
        if id[0]=='t':
            return self.__trelToDot(id,self.trelJ.get(id))
        if id[0]=='e':
            return self.__exprToDot(id,self.exprJ.get(id))
        return None


    def __relToDot(self,tblId,tblJ):

        if tblId in self.tblSaved:
            return tblId

        kindStr=tblJ.get('kind')

        #write to tblFile and colFile
        name=tblJ['name']
        lineStr='{}\t[shape=none label=<\n'\
        '\t<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">\n'\
        .format(tblId)
        self.tblFile.write(lineStr)
        self.colFile.write(lineStr)

        if kindStr=='tbl':
            lineStr='\t<TR><TD PORT="at" BGCOLOR="{}">{}</TD></TR>\n'.format('orange',name)
        else:
            lineStr='\t<TR><TD PORT="at" BGCOLOR="{}">{} {}</TD></TR>\n'.format('orange',kindStr,name)
        self.tblFile.write(lineStr)
        self.colFile.write(lineStr)

        colsJ=tblJ['cols']
        i=0
        for col in colsJ:
            colName=col.get('name')
            colType=col.get('type')
            if colType:
                lineStr='\t<TR><TD PORT="at{}" BGCOLOR="{}">{} {}</TD></TR>\n'\
                .format(i,'yellow',colName,colType)
            else:
                lineStr='\t<TR><TD PORT="at{}" BGCOLOR="{}">{}</TD></TR>\n'\
                .format(i,'yellow',colName)
            i+=1
            self.tblFile.write(lineStr)
            self.colFile.write(lineStr)

        self.tblFile.write('\t</TABLE>\n>];\n\n')
        self.colFile.write('\t</TABLE>\n>];\n\n')

        #this tbl has saved
        self.tblSaved.add(tblId)

        #track the from node in table level
        fromTblList=tblJ.get('from')
        for f in fromTblList:
            fIdStr=self._tblToDot(f)
            self._tblFromTo(fIdStr,tblId)

        #track the from node in column level
        i=0
        for col in colsJ:
            fJ=col.get('from')
            if fJ:
                colIdx=fJ.get('idx')
                if colIdx != None:
                    #is from one column of the table
                    self._colFromTo(fJ.get('id')+':at'+str(colIdx),tblId+':at'+str(i))
                else:
                    #is from one expr in column level
                    exprId=fJ.get('id')
                    exprIdStr=self.__cexprToDot(exprId)
                    self._colFromTo(exprIdStr,tblId+':at'+str(i))
            i+=1

        return tblId

    def __trelToDot(self,tblId,tblJ):
        if tblId in self.tblSaved:
            return tblId

        kindStr=tblJ.get('kind')
        if not kindStr=='FUNC':
            return self.__relToDot(tblId,tblJ)

        #write to tblFile and colFile
        name=tblJ['name']
        lineStr='{}\t[shape=none label=<\n'\
        '\t<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">\n'.format(tblId)
        self.tblFile.write(lineStr)
        self.colFile.write(lineStr)

        lineStr='\t<TR><TD COLSPAN="5" PORT="at" BGCOLOR="{}">{}</TD></TR>\n'.format('orange',name)
        self.tblFile.write(lineStr)
        self.colFile.write(lineStr)

        lineStr='\t<TR><TD COLSPAN="2" PORT="at_returns" BGCOLOR="{}">RETURNS</TD>\n'\
        '\t<TD COLSPAN="3" PORT="at_args" BGCOLOR="{}">ARGS</TD></TR>\n'.format('yellow','yellow')
        self.tblFile.write(lineStr)
        self.colFile.write(lineStr)

        argsCnt=tblJ.get('acnt')
        returnsCnt=tblJ.get('rcnt')
        argsJ=tblJ.get('args')
        returnsJ=tblJ.get('returns')
        it=0

        for arg,rt in zip(argsJ,returnsJ):

            lineStr='\t<TR><TD COLSPAN="2" ROWSPAN="{}" PORT="at{}" BGCOLOR="{}">{}</TD>\n'\
            '\t<TD COLSPAN="3" ROWSPAN="{}" PORT="at{}" BGCOLOR="{}">{}</TD></TR>\n'\
            .format(argsCnt,it,'yellow',rt.get('name'),returnsCnt,it+returnsCnt,'yellow',arg)
            self.tblFile.write(lineStr)
            self.colFile.write(lineStr)
            it+=1

        if it>=argsCnt:
            leftReturns=returnsJ[it:-1]
            for rt in leftReturns:
                lineStr='\t<TR><TD COLSPAN="2" ROWSPAN="{}" PORT="at{}" BGCOLOR="{}">{}</TD></TR>\n'\
                .format(argsCnt,it,'yellow',rt.get('name'))
                self.tblFile.write(lineStr)
                self.colFile.write(lineStr)
                it+=1
        else:
            leftArgs=argsJ[it:-1]
            for arg in leftArgs:
                lineStr='\t<TR><TD COLSPAN="3" ROWSPAN="{}" PORT="at{}" BGCOLOR="{}">{}</TD></TR>\n'\
                .format(returnsCnt,it+returnsCnt,'yellow',arg.get('info'))
                self.tblFile.write(lineStr)
                self.colFile.write(lineStr)
                it+=1

        self.tblFile.write('\t</TABLE>\n>];\n\n')
        self.colFile.write('\t</TABLE>\n>];\n\n')

        self.tblSaved.add(tblId)

        return tblId

    def __exprToDot(self,eId,eJ):
        algebra=eJ.get('algebra')
        info=eJ.get('info')

        if algebra=='Group By' or algebra=='groupBy':
            lineStr='%s\t[\tstyle="rounded,filled"\tfillcolor=%s\tshape=record'\
            '\n\tlabel="{ <f0> %s %s | <f1> %s }"\t];\n\n'\
             % (eId,'lightblue',algebra,_escapeDotStr(info[0]),_escapeDotStr(info[1]))
            self.tblFile.write(lineStr)
        elif algebra and info and len(info)>0:
            lineStr='%s\t[\tstyle="rounded,filled"\tfillcolor=%s\tshape=record'\
            '\n\tlabel="{ <f0> %s | <f1> %s }"\t];\n\n'\
             % (eId,'lightblue',algebra,_escapeDotStr(info[0]))
            self.tblFile.write(lineStr)
        elif algebra:
            lineStr='{}\t[\tstyle="rounded,filled"\tfillcolor={}\tshape=record'\
            '\n\tlabel="{}"\t];\n\n'\
            .format(eId,'lightblue',algebra)
            self.tblFile.write(lineStr)

        #track the from node in table level
        fromList=eJ.get('from')
        for f in fromList:
            fIdStr=self._tblToDot(f)
            self._tblFromTo(fIdStr,eId)

        return eId

    def __cexprToDot(self,eId):
        eJ=self.cexprJ[eId]
        info=eJ.get('info')
        froms=eJ.get('from')

        lineStr='{}\t[\tstyle="rounded,filled"\tfillcolor={}\tshape=record'\
        '\n\tlabel="{}"\t];\n\n'\
        .format(eId,'lightblue',_escapeDotStr(info))
        self.colFile.write(lineStr)

        for f in froms:
            fId=f.get('id')
            fIdx=f.get('idx')
            if fIdx!=None:
                #is from one column of the table
                self._colFromTo(fId+':at'+str(fIdx),eId)
            else:
                #is from one expr in column level
                exprIdStr=self.__cexprToDot(fId)
                self._colFromTo(exprIdStr,eId)

        return eId



def provQuery(dirPath,tbl,outputPath):
    isOk,status_or_statJ,msg_or_JsonList=yprovReadJson(dirPath)
    if not isOk:
        print(status_or_statJ,':',msg_or_relMapJ)
        return
    relMapJ,relJ,tRelJ,exprJ,cExprJ=msg_or_JsonList

    idx=relMapJ.get(tbl)
    if not idx:
        print(tbl,' doesnt exist in relmap')
        return

    dot=Dot(outputPath,relJ,tRelJ,exprJ,cExprJ)
    dot.relToDot(tbl,idx)


if __name__ == "__main__":
    import sys
    argLen=len(sys.argv)

    if argLen<3:
        print('usage:')
        print('for query table provenance:\n\t',sys.argv[0],'query dataPath query-table-name [outputPath]')
        print('or')
        print('for simply check the validity of data:\n\t',sys.argv[0],'chk dataPath')

    elif sys.argv[1]=='chk':
        simpleChk(sys.argv[2])
    elif argLen>3 and sys.argv[1]=='query':
        output='.'
        if argLen>4:
            output=sys.argv[4]
        provQuery(sys.argv[2],sys.argv[3],output)

    name=sys.argv[3]
    tblDotFilePath=os.path.join(output,name+'_tbl.png')
    colDotFilePath=os.path.join(output,name+'_col.png')
    cmdStr1='dot {}_tbl.dot -Tpng -o{}'.format(name,str(tblDotFilePath))
    cmdStr2='dot {}_col.dot -Tpng -o{}'.format(name,str(colDotFilePath))

    os.system(cmdStr1)
    os.system(cmdStr2)
