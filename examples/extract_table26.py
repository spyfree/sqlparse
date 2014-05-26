#!/usr/bin/python2.6


import sys
import re
import sqlparse
import string
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML,DDL,Whitespace,Newline,String

class sqlParser:

    def __init__(self):
        self.fromList = []
        self.toList = []
        self.oddList = []

    def is_subselect(self,parsed):
        if not parsed.is_group():
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
            if self.is_subselect(item):
                return True
        return False

    # return if is a sub_group other than "SELECT"
    def is_subchange(self,parsed):
        if parsed.is_group():
            for item in parsed.tokens:
                if (item.ttype is DML or item.ttype is DDL) and item.value.upper()!='SELECT':
                    return True
                if self.is_subchange(item):
                    return True
        return False

    def find_keyword(self,sql,keyword):
        keyword = keyword.upper()
        keyword = re.compile(r'\b%s\b' %keyword)
        match = re.search(keyword,sql)
        if match:
            return match.start()
        else:
            return -1

    def extract_to_part(self,parsed):
        if self.is_subchange(parsed.tokens[0]):
            for x in self.extract_to_part(parsed.tokens[0]):
                yield x
        to_seen = False
        for item in parsed.tokens:
            if to_seen:
                if item is None:
                    raise StopIteration
                elif item.ttype is Whitespace or item.ttype is Newline:
                    pass
                elif item.ttype is Keyword and item.value.upper() == 'IF':
                    post_item = parsed.token_next(parsed.token_index(item))
                    if post_item is not None and post_item.ttype is Keyword and post_item.value.upper() == 'EXISTS':
                        to_seen = False
                        yield parsed.token_next(parsed.token_index(post_item))
                else:
                    to_seen = False
                    yield item
            elif item.ttype is Keyword and re.match(r"(^.*TABLE$)|(^.*INTO$)",item.value.upper()): 
                pre_item = parsed.token_prev(parsed.token_index(item))
                if pre_item.ttype is Keyword and re.match(r"(^.*TEMP$)",pre_item.value.upper()):
                        pass#the create temp table case,ignored..
                else:
                    to_seen = True
                

    def extract_from_part(self,parsed):
        from_seen = False
        for item in parsed.tokens:
            if self.is_subselect(item):
                for x in self.extract_from_part(item):
                    yield x

            if item.ttype is None:  
                idx = self.find_keyword(unicode(item),'IN')
                if idx!=-1:
                    subsql = unicode(item)[idx+3:len(unicode(item))]
                    stmt = sqlparse.parse(subsql)[0] 
                    for x in self.extract_from_part(stmt):
                        yield x
                idx = self.find_keyword(unicode(item),'EXISTS')
                if idx != -1:
                    subsql = unicode(item)[idx+6:len(unicode(item))]
                    stmt = sqlparse.parse(subsql)[0] 
                    for x in self.extract_from_part(stmt):
                        yield x
                    
            if from_seen:
                if self.is_subselect(item):
                    for x in self.extract_from_part(item):
                        yield x
                    from_seen = False
                elif item is None:
                    raise StopIteration
                elif item.ttype is Whitespace or item.ttype is Newline:
                    pass
                else:
                    yield item
                    from_seen = False
            elif item.ttype is Keyword and re.match(r"(^.*FROM$)|(^.*JOIN$)|(\bUSING\b)",item.value.upper()):
                pre_item = parsed.token_prev(parsed.token_index(item))
                if pre_item.ttype is DML and re.match(r"(^.*DELETE$)",pre_item.value.upper()):
                    self.oddList.append(parsed.token_next(parsed.token_index(item)).value)#the delete from case,put the delete table to oddList
                else:
                    from_seen = True


    def extract_table_names(self,token_unicodeeam):
        for item in token_unicodeeam:
            if isinstance(item,IdentifierList):
                for identifier in item.get_identifiers():
                    tables = unicode.split(unicode(identifier))
                    for i in tables:
                        if re.match(r"(^.+\..+$)",unicode(i)):
                            if i[len(i)-1]=='(':
                                i = i[0:len(i)-1]
                            yield i	
            else:
                tables = unicode.split(unicode(item))
                for i in tables:
                    if re.match(r"(^.+\..+$)",unicode(i)):
                        if i[len(i)-1]=='(':
                            i = i[0:len(i)-1]
                        yield i

    def extract_from_tables(self,stmt):
        streamFrom = self.extract_from_part(stmt)
        return list(self.extract_table_names(streamFrom))

    def extract_to_tables(self,stmt):
        streamTo = self.extract_to_part(stmt)
        return list(self.extract_table_names(streamTo))


    def extractSingleSql(self,sql):
        sql = sqlparse.format(sql,reindent=True,keyword_case='upper')
        unicodeinfo = re.compile(r'\bAS\b')
        sql = unicodeinfo.sub(' ON ',sql) #temp convert as to on since splparse lib is not split 'as' keyword well
        stmt = sqlparse.parse(sql)[0]
        self.fromList.extend(self.extract_from_tables(stmt))
        self.toList.extend(self.extract_to_tables(stmt))

    def validateTables(self,table):
        db = re.split('\W+',table)[0]
        if db in self.db:
            return True
        else:
            return False



    def extractSql(self,text):
        self.toList=[]
        self.fromList=[]
        self.oddList=[]
        sqls = sqlparse.split(text)
        for sql in sqls:
            if len(sql)>0:
                self.extractSingleSql(sql)

        self.toList.extend(self.oddList)
        self.fromList = list(set(self.fromList))
        self.toList = list(set(self.toList))
        self.fromList = list(filter(self.validateTables,self.fromList))
        self.toList = list(filter(self.validateTables,self.toList))
        return [self.fromList,self.toList]



if __name__ == '__main__':
    f = open(sys.argv[1],'r')
    sqls_text = f.read()
    x = sqlParser()
    print(x.extractSql(sqls_text))

        
