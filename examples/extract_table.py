

import sys
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML,Whitespace,Newline


def is_subselect(parsed):
   # print('subset'+str(parsed))
    if not parsed.is_group():
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item is None:
                raise StopIteration
            elif item.ttype is Whitespace or item.ttype is Newline:
                pass
            else:
                yield item
                from_seen = False
        elif item.ttype is Keyword and re.match(r"(^.*FROM$)|(^.*JOIN$)",item.value.upper()):
            from_seen = True

def extract_join_part(parsed):
	join_seen = False
	for item in parsed.tokens:
		if join_seen:
			if is_subselect(item):
				for x in extract_join_part(item):
					yield x
			elif item.ttype is Keyword:
				raise StopIteration
			else:
				yield item
		elif item.ttype is Keyword and re.match(r"(^.*JOIN$)",item.value.upper()):
				join_seen = True

def extract_table_names(token_stream):
	for item in token_stream:
		if isinstance(item,IdentifierList):
			for identifier in item.get_identifiers():
				tables = str.split(str(identifier))
				for i in tables:
					if re.match(r"(^.+\..+$)",str(i)):
						print(i)
		else:
			tables = str.split(str(item))
			for i in tables:
					if re.match(r"(^.+\..+$)",str(i)):
						print(i)

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


def extract_tables(sql):
    #tokens = sqlparse.parse(sql)[0].tokens
    #for i in tokens:
    #    print(str(i)+'#'+str(i.ttype))
    streamFrom = extract_from_part(sqlparse.parse(sql)[0])
##    streamJoin = extract_join_part(sqlparse.parse(sql)[0])
    extract_table_names(streamFrom)
 ##   extract_table_names(streamJoin)
    return list(extract_table_identifiers(streamFrom))


if __name__ == '__main__':
	f = open(sys.argv[1],'r')
	sql = f.read()
	print(extract_tables(sql))
