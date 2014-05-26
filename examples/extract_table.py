#!/usr/local/bin/python3.4

import sys
import re
import sqlparse
import string
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML,DDL,Whitespace,Newline,String

fromList = []
toList = []
oddList=[]
functionList=[]

def is_subselect(parsed):
    if not parsed.is_group():
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
        if is_subselect(item):
            return True
    return False

# return if is a sub_group other than "SELECT"
def is_subchange(parsed):
	if parsed.is_group():
		for item in parsed.tokens:
			if (item.ttype is DML or item.ttype is DDL) and item.value.upper()!='SELECT':
				return True
			if is_subchange(item):
				return True
	return False

def find_keyword(sql,keyword):
	keyword = keyword.upper()
	keyword = re.compile(r'\b%s\b' %keyword)
	match = re.search(keyword,sql)
	if match:
		return match.start()
	else:
		return -1

def extract_to_part(parsed):
	if is_subchange(parsed.tokens[0]):
		for x in extract_to_part(parsed.tokens[0]):
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
			

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if is_subselect(item):
            for x in extract_from_part(item):
                yield x

        if item.ttype is None:  
            print(str(item))
            idx = find_keyword(str(item),'IN')
            if idx!=-1:
                subsql = str(item)[idx+3:len(str(item))]
                stmt = sqlparse.parse(subsql)[0] 
                for x in extract_from_part(stmt):
                    yield x
            idx = find_keyword(str(item),'EXISTS')
            if idx != -1:
                subsql = str(item)[idx+6:len(str(item))]
                stmt = sqlparse.parse(subsql)[0] 
                for x in extract_from_part(stmt):
                    yield x
				
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
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
                oddList.append(parsed.token_next(parsed.token_index(item)).value)#the delete from case,put the delete table to oddList
            else:
                from_seen = True


def extract_table_names(token_stream):
    for item in token_stream:
        if isinstance(item,IdentifierList):
            for identifier in item.get_identifiers():
                tables = str.split(str(identifier))
                for i in tables:
                    if re.match(r"(^.+\..+$)",str(i)):
                        if i[len(i)-1]=='(':                                                                                                                                    
                            i = i[0:len(i)-1]
                        yield i	
        else:
            tables = str.split(str(item))
            for i in tables:
                if re.match(r"(^.+\..+$)",str(i)):
                    if i[len(i)-1]=='(':                                                                                                                                    
                        i = i[0:len(i)-1]
                    yield i

def extract_from_tables(stmt):
    streamFrom = extract_from_part(stmt)
    return list(extract_table_names(streamFrom))

def extract_to_tables(stmt):
    streamTo = extract_to_part(stmt)
    return list(extract_table_names(streamTo))


def extractSingleSql(sql):
    sql = sqlparse.format(sql,reindent=True,keyword_case='upper')
    strinfo = re.compile(r'\bAS\b')
    sql = strinfo.sub(' ON ',sql) #temp convert as to on since splparse lib is not split 'as' keyword well
    stmt = sqlparse.parse(sql)[0]
    fromList.extend(extract_from_tables(stmt))
    toList.extend(extract_to_tables(stmt))


if __name__ == '__main__':
    f = open(sys.argv[1],'r')
    sqls_text = f.read()

    sqls = sqlparse.split(sqls_text)	
    for sql in sqls:
        if len(sql)>0:
            extractSingleSql(sql)
    #for i in stmt.tokens:
    #	print(str(i)+'#'+str(i.ttype))
	
    toList.extend(oddList)

    fromList = list(set(fromList))
    toList = list(set(toList))

    print('from tables:',fromList)
    print('to tables:',toList)
