import sqlite3


def create_connection():
    con = sqlite3.connect('../scripts/uncommitted/warehouse.db')
    return con


def drop_table(con):
    drop_pos = """ Drop table IF EXISTS posts"""
    drop_tag = """ Drop table IF EXISTS tags"""
    con.execute(drop_pos)
    con.execute(drop_tag)
    con.commit()


def create_table(con):
    create_posts = '''
            CREATE TABLE IF NOT EXISTS posts (
            Id text,
            PostTypeId text,
            AcceptedAnswerId text,
            CreationDate text,
            Score text,
            ViewCount text,
            Body text,
            AnswerCount text,
            CommentCount text,
            FavouriteCount text,
            ContentLicense text,
            ClosedDate text,
            LastActivityDate text,
            CommunityOwnedDate text,
            LastEditDate text,
            OwnerDisplayName text,
            Tags text,
            LastEditorUserId text,
            OwnerUserId text,
            FavoriteCount text,
            ParentId text,
            Title text,
            LastEditorDisplayName text
            )
            '''
    create_tags = '''
                CREATE TABLE IF NOT EXISTS tags (
                    Id text,
                    TagName text,
                    Count text,
                    ExcerptPostId text,
                    WikiPostId text
                )
                '''
    cursor = con.cursor()
    cursor.execute(create_posts)
    cursor.execute(create_tags)
    con.commit()


def insert_data(con, root_element, table_name):
    """
    Insert into the parameterised table
    :param table_name: posts or tags
    :param con: connection string
    :param root_element: element of xml , posts or tags
    """
    column_str = ''  # to collect no of ? for insert statement
    record = {}  # collect xml elements in each iteration

    for i in range(0, len(tuple(root_element.keys()))):
        if column_str == '':
            column_str = '?'
        else:
            column_str = column_str + ',?'
        record[root_element.keys()[i]] = root_element.attrib[root_element.keys()[i]]

    insert_sql = (''' INSERT INTO {}(''' + ','.join(tuple(record.keys())) + ') VALUES(' + column_str + ') '''). \
        format(table_name)

    cur = con.cursor()
    cur.execute(insert_sql, tuple(record.values()))
    con.commit()
    return cur.lastrowid


def read_data(con, table_name, rowid):
    """
    Read a sample record from the posts and tags table
    :param rowid: last rowid of inserted records in the table posts or tags passed from main.py
    :param table_name: table name posts or tags passed from main.py
    :param con: sqlite3 connection for warehouse.db
    """
    read_record = ''' select * from {} where rowid = {}'''.format(table_name, rowid)
    cur = con.cursor()
    cur.execute(read_record)
    result_pos = cur.fetchone()
    con.commit()
    #con.close()
    return result_pos


