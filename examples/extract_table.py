#!/usr/local/bin/python3.4

import sys
import re
import sqlparse
import string
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML,DDL,Whitespace,Newline,String

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
					pass
			else:
				to_seen = True
			

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if is_subselect(item):
            for x in extract_from_part(item):
                yield x
        if item.ttype is None:  
            idx = find_keyword(str(item),'IN')
            if idx!=-1:
                subsql = str(item)[idx+3:len(str(item))]
                #print(subsql+'hahah')
                stmt = sqlparse.parse(subsql)[0] 
                for x in extract_from_part(stmt):
                    yield x
            idx = find_keyword(str(item),'EXISTS')
            if idx != -1:
                subsql = str(item)[idx+6:len(str(item))]
                #print(subsql+'hahahhahaha')
                stmt = sqlparse.parse(subsql)[0] 
                for x in extract_from_part(stmt):
                    yield x
				
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item is None:
                raise StopIteration
            elif item.ttype is Whitespace or item.ttype is Newline:
                pass
            else:
                #print(str(item)+"hahahahah")
                yield item
                from_seen = False
        elif item.ttype is Keyword and re.match(r"(^.*FROM$)|(^.*JOIN$)|(\bUSING\b)",item.value.upper()):
            pre_item = parsed.token_prev(parsed.token_index(item))
            if pre_item.ttype is DML and re.match(r"(^.*DELETE$)",pre_item.value.upper()):
                oddList.append(parsed.token_next(parsed.token_index(item)).value)
            else:
                from_seen = True


def extract_table_names(token_stream):
	for item in token_stream:
		if isinstance(item,IdentifierList):
			for identifier in item.get_identifiers():
				tables = str.split(str(identifier))
				for i in tables:
					if re.match(r"(^.+\..+$)",str(i)):
						yield i	
		else:
			tables = str.split(str(item))
			for i in tables:
					if re.match(r"(^.+\..+$)",str(i)):
						yield i

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value


def extract_from_tables(stmt):
    streamFrom = extract_from_part(stmt)
    return list(extract_table_names(streamFrom))

def extract_to_tables(stmt):
    streamTo = extract_to_part(stmt)
    return list(extract_table_names(streamTo))


if __name__ == '__main__':
	f = open(sys.argv[1],'r')
	sql = f.read()
	
	sql = sqlparse.format(sql,reindent=True,keyword_case='upper')
	print(sql)
	strinfo = re.compile(r'\bAS\b')
	sql = strinfo.sub(' ON ',sql) #temp convert as to on
	
	stmt = sqlparse.parse(sql)[0]
	#for i in stmt.tokens:
	#	print(str(i)+'#'+str(i.ttype))
	
	table_names_from = extract_from_tables(stmt)
	table_names_to = extract_to_tables(stmt)

	table_names_from = list(set(table_names_from))
	table_names_to = list(set(table_names_to))

	print('from tables:',table_names_from)
	print('to tables:',table_names_to)
	print('odd List:',oddList)
