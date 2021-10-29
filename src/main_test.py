import defusedxml.ElementTree as Et

import main
import db


def test_to_sql_post_over_example_data():
    example_data = '''<?xml version="1.0" encoding="utf-8"?>
<row Id="1" PostTypeId="1" AcceptedAnswerId="3" CreationDate="2016-08-02T15:39:14.947" /> '''
    test_rowid = main.to_sql(main.conn, Et.fromstring(example_data), main.table_post)

    assert db.read_data(main.conn, main.table_post, test_rowid) == ('1', '1', '3', '2016-08-02T15:39:14.947', None,
                                                                    None, None, None, None, None, None, None,
                                                                    None, None, None, None, None, None, None, None,
                                                                    None, None, None)


def test_to_sql_tag_over_example_data():
    example_data = '''<?xml version="1.0" encoding="utf-8"?>
<row Id="2" TagName="generalization" Count="25" ExcerptPostId="17536" WikiPostId="17535" />'''
    test_rowid = main.to_sql(main.conn, Et.fromstring(example_data), main.table_tag)
    assert db.read_data(main.conn, main.table_tag, test_rowid) == ('2', 'generalization', '25', '17536', '17535')
