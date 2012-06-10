'''
Created on 10/06/2012

@author: piranna
'''

from unittest import main, TestCase

import sqlparse


class Issue_66(TestCase):
    """
    Attempts to extract the column names from an SQL dump.

    Demonstrates a bug in sqlparse.
    """

    def test_issue(self):
        # example sql, taken from a dump file I'm working with.
        sql = r"""
CREATE TABLE `foo` (
  `id` smallint(6) NOT NULL AUTO_INCREMENT,
  `user_id` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
)
"""

        # first, parse the CREATE statement
        statement, = sqlparse.parse(sql)

        # the name of the table is followed by a pair of parentheses
        # defining the column names and data types. sqlparse interprets
        # this as a function.
        function, = [tok for tok in statement.tokens \
                        if type(tok) is sqlparse.sql.Function]

        # this 'function' has three tokens: the table name, a whitespace,
        # and finally a parenthesis.
        parenthesis = function.tokens[-1]

        # BUG: sqlparse fails to isolate certain tokens of this parenthesis.
        # for example, the name of the second table column `user_id` is lumped
        # into an IdentifierList with the AUTO_INCREMENT keyword on the preceding
        # line, rather than being recognized as another Name (as is the first
        # column name in the list).

        # to demonstrate this, print the tokens one by one.
        # the error is visible on Line 9.
        for i, token in enumerate(parenthesis.tokens):
            print i, repr(token)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    main()