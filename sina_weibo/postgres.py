'''
Created on Jun 21st, 2015

Postgres API

@author: surfer
'''

class db:

	def __init__(self, conn):
		self.conn = conn
		
	# insert data (pairs of column and value) into table
	def Insert(self, table, data):
		strCol = ''
		strPlaceholders = ''
		dataValues = []
		for k in data.keys():
			if data[k]:
				strCol += ',\"' + k + '\"'
				if isinstance(data[k], list):
					dataValue = '|'.join(data[k])
				else:
					dataValue = data[k]

				if not isinstance(dataValue, unicode): # include orindary string, integer etc.
					try:
						dataValue = dataValue.decode('utf-8')
					except:
						pass

					dataValue = unicode(dataValue)
					
				strPlaceholders += "," + "%s"
				dataValues.append(dataValue.encode('utf-8'))
		
		qs = "INSERT INTO %s ("  % table + strCol[1:] + ") VALUES (" + strPlaceholders[1:] + ")"
		with self.conn.cursor() as cur:
			cur.execute(qs, dataValues)

	
	def Update(self, table, updatedata, wheredict):
		
		where = '1=1'
		for k in wheredict.keys():
			if not isinstance(wheredict[k], unicode): # include orindary string, integer etc.
				try:
					wheredict[k] = wheredict[k].decode('utf-8')
				except:
					pass
	
			wheredict[k] = unicode(wheredict[k])
			
			where += " AND "+k+"='"+wheredict[k]+"'"
		updatestr = ''
		for k in updatedata.keys():
			if updatedata[k] and not isinstance(updatedata[k], unicode): # include orindary string, integer etc.
				try:
					updatedata[k] = updatedata[k].decode('utf-8')
				except:
					pass
	
				updatedata[k] = unicode(updatedata[k])

			if updatedata[k]:
				updatestr += ","+k+"='"+updatedata[k]+"'"
		
		qs = "UPDATE "+table+" SET "+updatestr[1:]+" WHERE "+where

		with self.conn.cursor() as cur:
			cur.execute(qs)
	
	def getCount(self, dbname, wheredict):
		
		where = '1'
		for k in wheredict.keys():
			where += " AND `"+k+"`='"+self.conn.escape_string(str(wheredict[k]))+"'"
			
		qs = "SELECT COUNT(*) FROM `"+dbname+"` WHERE "+where
		self.conn.query(qs)
		r=self.conn.store_result()
		tup = r.fetch_row()
		return int(tup[0][0])
		
	def getAll(self, qs):
		with self.conn.cursor() as cur:
			cur.execute(qs)
			return cur.fetchall()
	
	def getOne(self, qs):
		with self.conn.cursor() as cur:
			cur.execute(qs)
			result = cur.fetchone()
		return result

	def check_crawled_url(self, url_table, url_filter):
		
		cur = self.conn.cursor()
		cur.execute("select 'dummy value' from %s where %s limit 1" % (url_table, url_filter))
		rows = cur.fetchall()
		
		if rows:
			is_url_found = True
		else:
			is_url_found = False
		
		return is_url_found
