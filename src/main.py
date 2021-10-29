"""Entrypoint to populate the database"""
import defusedxml.ElementTree as Et
import db

tree = Et.parse('../scripts/uncommitted/Posts.xml')  # parse Posts.xml
tree_tag = Et.parse('../scripts/uncommitted/Tags.xml') # parse Tags.xml

table_post = 'posts'
table_tag = 'tags'


def to_sql(con, element, table_name):
    return db.insert_data(con, element, table_name)


conn = db.create_connection()
db.create_table(conn)

# iterate through the post tree for insert
for elem in tree.getroot():
    post = to_sql(conn, elem, table_post)
print(" Last Row_id inserted for table posts")
print(post)  # printing the last rowid inserted in the table posts


# iterate through the tag tree for insert
for elem in tree_tag.getroot():
    tag = to_sql(conn, elem, table_tag)
print(" Last Row_id inserted for table tags")
print(tag)  # printing the last rowid inserted in the table tags


# Read the sample record from the tables  for above collected rowid
print(" last rowid sample from table post")
print(db.read_data(conn, table_post, post))
print(" last rowid sample from table tags")
print(db.read_data(conn, table_tag, tag))
