# library
from google.appengine.ext import ndb

# set up class for looking up url mapping entry from the Datatstore
class Model(ndb.Model):
	code = ndb.StringProperty(indexed=True)
	attr1 = ndb.StringProperty(indexed=False)