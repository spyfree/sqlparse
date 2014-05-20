#!/usr/local/bin/python3.4

import sys
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML,Whitespace,Newline,String


def is_subselect(parsed):
    print(str(parsed)+" parsed")
    if not parsed.is_group():
        print("not group")
        return False
    for item in parsed.tokens:
        print(str(item)+ ' sub')
        if item.ttype is DML and item.value.upper() == 'SELECT':
            print('select')
            return True
    return False

def is_subchange(parsed):
	if parsed.is_group():
		for item in parsed.tokens:
			if (item.ttype is DML or item.ttype is DDL) and not is_subselect(parsed):
				return True
	return False

def extract_to_part(parsed):
	to_seen = False
	for item in parsed.tokens:
		if is_subselect(item):
			print(item.value+'set')
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
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                raise StopIteration
            elif item.ttype is Whitespace or item.ttype is Newline:
                pass
            else:
                yield item
                from_seen = False
        elif item.ttype is Keyword and re.match(r"(^.*FROM$)|(^.*JOIN$)",item.value.upper()):
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


def extract_from_tables(sql):
    stmt = sqlparse.parse(sql)[0]
    tokens = stmt.tokens
    #for i in tokens:
    #    print(str(i)+'#'+str(i.ttype))
    streamFrom = extract_from_part(stmt)
    return list(extract_table_names(streamFrom))

def extract_to_tables(sql):
    stmt = sqlparse.parse(sql)[0]
    streamTo = extract_to_part(stmt)
    return list(extract_table_names(streamTo))

def extract_tables(sql):
	stmt = sqlparse.parse(sql)[0]
	tokens = stmt.tokens


if __name__ == '__main__':
	f = open(sys.argv[1],'r')
	sql = f.read()
	table_names_from = extract_from_tables(sql)
	#table_names_to = extract_to_tables(sql)
	table_names_from = list(set(table_names_from))
	#table_names_to = list(set(table_names_to))
	print('from tables:',table_names_from)
	#print('to tables:',table_names_to)
