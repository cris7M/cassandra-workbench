# Developed by sanjay kumar
# Email: krishnasanjay010@gmail.com
import uuid
from flask import Flask,render_template,request,redirect,jsonify,json
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory
from cassandra import metadata
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine import columns, query
from cassandra.cqlengine.connection import execute, get_cluster, format_log_context
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.usertype import UserType


cluster = Cluster(['127.0.0.1'])
session = cluster.connect('system')

app= Flask(__name__)
app.config['APPLICATION_ROOT'] = '/'

@app.route('/')
def index():
	session = cluster.connect('system')
	data= session.execute("select keyspace_name from system_schema.keyspaces")
	return render_template('code.html',data=data)

@app.route('/callTable', methods=['GET'])
def callTable():
	keyspace_name=request.args.get('test');
	session = cluster.connect(keyspace_name)
	data= session.execute("select keyspace_name from system_schema.keyspaces")
	value = session.execute("select table_name from system_schema.tables where keyspace_name='{}'".format(keyspace_name))
	print(value)
	return render_template('code.html',value=value,data=data,keyspace_name=keyspace_name)

@app.route('/getTabCol/', methods=['GET'])
def getTabCol():
	keyspace_name=request.args.get('echoValue');
	table_name=request.args.get('echoValue1');
	session = cluster.connect(keyspace_name)
	session.row_factory = ordered_dict_factory
	# test=session.execute("select column_name from system_schema.columns where keyspace_name='{}' and table_name='{}'".format(keyspace_name,table_name))
	test = cluster.metadata.keyspaces[keyspace_name].tables[table_name].columns	
	values = []
	for i in test:
		values.append(i)
	print("$$$$$$",values)
	return jsonify(data=values)

@app.route('/getTablePrimaryKey/', methods=['GET'])
def getTablePrimaryKey():
	table_name=request.args.get('echoValue');
	keyspace_name=request.args.get('echoValue1');
	session = cluster.connect(keyspace_name)
	session.row_factory = ordered_dict_factory
	# test=session.execute("select column_name from system_schema.columns where keyspace_name='{}' and table_name='{}'".format(keyspace_name,table_name))
	test = cluster.metadata.keyspaces[keyspace_name].tables[table_name].primary_key
	priCol=[]
	for i in test:
		priCol.append(i.name)
	return jsonify(data=priCol)

@app.route('/getPartition/', methods=['GET'])
def getPartition():
	table_name=request.args.get('table');
	keyspace_name=request.args.get('keyspace');
	session = cluster.connect(keyspace_name)
	session.row_factory = ordered_dict_factory
	# test=session.execute("select column_name from system_schema.columns where keyspace_name='{}' and table_name='{}'".format(keyspace_name,table_name))
	test = cluster.metadata.keyspaces[keyspace_name].tables[table_name].partition_key
	k=0
	for i in test:
		k=k+1
	return jsonify(data=k)


@app.route('/getTableProp/', methods=['GET'])
def getTableProp():
	table_name=request.args.get('echoValue');
	keyspace_name=request.args.get('echoValue1');
	session = cluster.connect(keyspace_name)
	session.row_factory = ordered_dict_factory
	test=session.execute("select column_name,type,kind from system_schema.columns where keyspace_name='{}' and table_name='{}'".format(keyspace_name,table_name))
	values = []
	k=0
	for i in test:
		values.append(i.values())

	print(values)
	return render_template("popUp.html",rows=values,test="value1",t_name=table_name)	

@app.route('/postCode', methods = ['GET','POST'])
def postCode():
	if request.method == 'POST':
		keyspace=request.form['keyspace'].strip()
		tableName=request.form['tableVal'].strip()
		if(keyspace != '' and tableName != ''):
			session = cluster.connect(keyspace)
			print(keyspace)
			data= session.execute("select keyspace_name from system_schema.keyspaces")
			value = session.execute("select table_name from system_schema.tables where keyspace_name='{}'".format(keyspace))
			try:
				msg=request.form['preview'].strip()
				print(msg)
				if 'insert' in msg:
					if tableName in msg:
						session.execute(msg)
						session.row_factory = ordered_dict_factory
						d_rows=session.execute("select *from {}".format(tableName))
						print(d_rows)
						keys = []
						values = []
						k=0
						for i in d_rows:
							keys=i.keys()
							print(keys)
							values.append(i.values())
							print(values)
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template("code.html",codeValue=msg,test="value",rr="msg",data=data,keys=keys,rows=values,value=value,keyspace_name=keyspace,tableName=tableName,message="Data Inserted Successfully",prim=priCol)
					else:
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template("code.html",codeValue=msg,test="value",rr="error",data=data,value=value,keyspace_name=keyspace,tableName=tableName,message="Error",prim=priCol)
				elif 'delete' in msg:
					if tableName in msg:
						session.execute(msg)
						session.row_factory = ordered_dict_factory
						d_rows=session.execute("select *from {}".format(tableName))
						print(d_rows)
						keys = []
						values = []
						k=0
						for i in d_rows:
							keys=i.keys()
							print(keys)
							values.append(i.values())
							print(values)
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template("code.html",codeValue=msg,test="value",rr="msg",data=data,keys=keys,rows=values,value=value,keyspace_name=keyspace,tableName=tableName,message="Row Deleted Successfully",prim=priCol)				
					else:
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template("code.html",codeValue=msg,test="value",rr="error",data=data,value=value,keyspace_name=keyspace,tableName=tableName,message="Error",prim=priCol)
				elif 'update' in msg:
					if tableName in msg:
						session.execute(msg)
						session.row_factory = ordered_dict_factory
						d_rows=session.execute("select *from {}".format(tableName))
						print(d_rows)
						keys = []
						values = []
						k=0
						for i in d_rows:
							keys=i.keys()
							print(keys)
							values.append(i.values())
							print(values)
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template("code.html",codeValue=msg,test="value",rr="msg",data=data,keys=keys,rows=values,value=value,keyspace_name=keyspace,tableName=tableName,message="Row Updated Successfully ",prim=priCol)				
					else:
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template("code.html",codeValue=msg,test="value",rr="error",data=data,value=value,keyspace_name=keyspace,tableName=tableName,message="Error")
				elif 'truncate' in msg:
					session.execute(msg)
					prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
					priCol=[]
					for i in prim:
						priCol.append(i.name)
					return render_template("code.html",codeValue=msg,test="value",rr="msg",data=data,value=value,keyspace_name=keyspace,tableName=tableName,message="All Record Deleted Successfully",prim=priCol)
				else:
					if tableName in msg:
						session.row_factory = ordered_dict_factory
						d_rows=session.execute(msg)
						print("#################",d_rows)
						keys = []
						values = []
						k=0
						for i in d_rows:
							keys=i.keys()
							values.append(i.values())
							print(i.keys(),'-------',values)
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template('code.html',codeValue=msg,test="value",rr="msg",keys=keys,rows=values, message="Tables",data=data,value=value,keyspace_name=keyspace,tableName=tableName,prim=priCol)
					else:
						prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
						priCol=[]
						for i in prim:
							priCol.append(i.name)
						return render_template("code.html",codeValue=msg,test="value",rr="error",data=data,value=value,keyspace_name=keyspace,tableName=tableName,message="Error",prim=priCol)
			except:
				prim = cluster.metadata.keyspaces[keyspace].tables[tableName].primary_key
				priCol=[]
				for i in prim:
					priCol.append(i.name)
				return render_template('code.html',rr="error",message="Error",data=data,value=value,keyspace_name=keyspace,tableName=tableName,prim=priCol)
		else:
			session = cluster.connect('system')
			print(keyspace)
			data= session.execute("select keyspace_name from system_schema.keyspaces")
			return render_template('code.html',rr="error",data=data,message="Error")	

if __name__ == '__main__':
	app.run(debug=True,host='0.0.0.0',port=5160)
