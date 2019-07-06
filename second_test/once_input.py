import zlib
import os
import zipfile
import rarfile
import pymysql
import traceback
import time
from lxml import etree
import re
import shutil
from warnings import filterwarnings
from datetime import datetime,date,timedelta
import chardet
import sys
filterwarnings('error', category = pymysql.Warning)

# 本程序分为导入销项数据或导入申报数据
# 参数为'xx'表示导入销项数据。
# 参数为'sb'表示导入申报数据。


conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='yst@2019',db='rdc')
cur = conn.cursor()

def file_preprocess(file_path):
	# 识别编码和换行符
    # os.path.getsize是指获取文件字节大小
	if(os.path.getsize(file_path)<=1):
		return 'gbk','\r\n'
	charset = "utf8"
	try:
		charset = "utf8"
		f = open(file_path,'r',encoding = charset,newline = '')
		text = f.read()
	except UnicodeDecodeError as e:
		try:
			charset = "gbk"
			f = open(file_path,'r',encoding = charset,newline = '')
			text = f.read()
		except UnicodeDecodeError as e:
			print("Can not detect char type!")
	# print("Char type is "+charset)
	
	lineBreak = r"\r\n"
	# f = open(file_path,'r',encoding = charset)
	# text = f.read()
	if(text.find("\"\r\n\"")>=0):
		lineBreak = r"\r\n"
	elif(text.find("\"\r\"")>=0):
		lineBreak = r"\r"
	elif(text.find("\"\n\"")>=0):
		lineBreak = r"\n"
	
	pattern = re.compile(r'[^"]\n[^"]')
	subStrL = list(set(pattern.findall(text)))
	if(len(subStrL)>0):
		for subStr in subStrL:
			text = text.replace(subStr,subStr[:1]+" "+subStr[-1:])
		f = open(file_path, 'w',encoding = charset)
		f.write(text)
		f.close()
		# charset = 'gbk'
		# f = open(file_path,'r',encoding = charset)
		# text = f.read()
		lineBreak = r"\r\n"
	else:
		charset = 'utf8'
	
	return charset,lineBreak

def get_page(file_path):
	try:
		charset = "gbk"
		f = open(file_path,'r',encoding = charset)
		text = f.read()
	except UnicodeDecodeError as e:
		try:
			charset = "utf8"
			f = open(file_path,'r',encoding = charset)
			text = f.read()
		except UnicodeDecodeError as e:
			print("Can not detect char type!")
	page = etree.HTML(text)
	return page

def all_analysis(file_path):
	
	report_typ = 0
	if(file_path.find(u"2013版财务报表")>=0):
		report_typ = 1
	elif(file_path.find(u"增值税.")>=0):
		report_typ = 2
	else:
		return -1
	print(file_path)
	# f = open(file_path,'rb') 
	# buf = f.read()
	# charRes = chardet.detect(buf)
	# f = open(file_path,'r',encoding = charRes["encoding"])
	# # f=open(file_path,'r')
	# page = etree.HTML(f.read())
	page = get_page(file_path)
	try:
		ent_id = page.xpath('/html//table[1]//tr[3]/td[1]/b/text()')[0]
		ent_id = ent_id.split("：")[1]
		list = page.xpath('/html//table[2]//tr')
	except Exception as e:
		print(str(datetime.now())+"\n"+"Error:"+str(e)+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 41. Parse table header error.\n\n")
		return -1
	
	detail_length = 0
	for detail in list:
		detail_length+=1
		if(detail_length==1):
			continue
		items = detail.xpath('./td')
		if(len(items)<7):
			continue
		item_content = []
		item_content.append(ent_id)
		item_length = 0
		for item in items:
			item_length += 1
			if(item_length==7):
				continue
			content = item.xpath('./div/text()')[0]
			content = content.replace("\u3000","")
			item_content.append(content)
			if(item_length==5):
				cycle_typ = report_cycle_typ(item_content[-2],item_content[-1])
				item_content.append(cycle_typ)
		item_str = "insert ignore into l1_all_list values(sysdate(),'"+"','".join(item_content)+"');"
		if(len(item_content)==8):
			mysql_execute(item_str,file_path)
		
		if(item_content[2].find(u"小规模纳税人适用")>=0):
			report_typ = 3
		elif(item_content[2].find(u"一般纳税人适用")>=0):
			report_typ = 2
		elif(item_content[2].find(u"增值税预缴税款表")>=0):
			report_typ = 4
		if(report_typ==1): # 财务报表，包含资产负债表和利润表
			next_file_path = file_path.split(".")[0]+"."+item_content[1]
			# if(os.path.exists(next_file_path+".1.html")):
				# balance_sheet_analysis(next_file_path+".1.html",ent_id,item_content[4],item_content[5])
			# if(os.path.exists(next_file_path+".2.html")):
				# profit_statement_analysis(next_file_path+".2.html",ent_id,item_content[4],item_content[5])
		elif(report_typ==2): # 一般纳税人的增值税申报表
			next_file_path = file_path.split(".")[0]+"."+item_content[1]
			if(os.path.exists(next_file_path+".1.html")):
				value_added_tax_analysis(next_file_path+".1.html") # 增值税总表
			if(os.path.exists(next_file_path+".3.html")):
				purchase_tax_analysis(next_file_path+".3.html",ent_id) # 进项税额统计表
			if(os.path.exists(next_file_path+".4.html")):
				pass
				purchase_detail_analysis(next_file_path+".4.html") # 进项抵扣明细表
		elif(report_typ==3): # 小规模纳税人适用增值税申报表
			next_file_path = file_path.split(".")[0]+"."+item_content[1]
			if(os.path.exists(next_file_path+".1.html")):
				value_added_tax_analysis_small_scale(next_file_path+".1.html")
		elif(report_typ==4): # 增值税预缴税款表
			print(u"增值税预缴税款表")

def value_added_tax_analysis(file_path):
	print(file_path)
	# f = open(file_path,'rb') 
	# buf = f.read()
	# charRes = chardet.detect(buf)
	# f = open(file_path,'r',encoding = charRes["encoding"])
	# # f=open(file_path,'r')
	# page = etree.HTML(f.read())
	page = get_page(file_path)
	try:
		tax_date_section = page.xpath('//*[@id="MyDiv"]/table[1]//tr[5]/td[1]/text()')[0]
	except Exception as e:
		print("Error:"+str(e)+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 184. Table error. "+str(e)+"\n\n")
		return -1
	
	date_list = re.findall(r"\d+",tax_date_section)
	if(len(date_list)==6):
		begin_date = date_list[0]+"-"+date_list[1]+"-"+date_list[2]
		end_date = date_list[3]+"-"+date_list[4]+"-"+date_list[5]
	else:
		print("Error when parse:"+file_path+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 194. Parse table period error.\n\n")
		return -1
	cycle_typ = report_cycle_typ(begin_date,end_date)
	
	ent_id = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td[2]/text()')[0]
	ent_nm = page.xpath('//*[@id="MyDiv"]/table[2]//tr[2]/td[2]/text()')[0]
	ent_indu = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td[4]/text()')[0]
	ent_type = page.xpath('//*[@id="MyDiv"]/table[2]//tr[3]/td[5]/text()')[0]
	
	list = page.xpath('//*[@id="MyDiv"]/table[3]//tr')
	
	detail_length = 0
	for detail in list:
		detail_length+=1
		if(detail_length<3 or detail_length >40):
			continue
		items = detail.xpath('./td')
		item_str = "insert ignore into  l1_value_added_tax values(sysdate(),'"+ent_id+"','"+begin_date+"','"+end_date+"','"+cycle_typ+"'"
		item_length = 0
		for item in items:
			item_length += 1
			if(len(items)>6 and item_length==1):
				continue
			content = item.xpath('./text()')[0]
			content = content.replace("\u3000","").replace("\xa0","")
			content = content.replace("\n","")
			if(len(items)==6 and item_length==2 and len(content)>2):
				content = content[:2]
			item_str = item_str + ",'"+content+"'"
		item_str = item_str+");"
		mysql_execute(item_str,file_path)

def value_added_tax_analysis_small_scale(file_path):
	# print(file_path)
	# f = open(file_path,'rb') 
	# buf = f.read()
	# charRes = chardet.detect(buf)
	# f = open(file_path,'r',encoding = charRes["encoding"])
	# # f=open(file_path,'r')
	# page = etree.HTML(f.read())
	page = get_page(file_path)
	
	try:
		tax_date_section = page.xpath('//*[@id="MyDiv"]/table[2]//tr[3]/td[1]/text()')[0]
	except Exception as e:
		print("Error:"+str(e)+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 236. Table error. "+str(e)+"\n\n")
		return -1
	
	date_list = re.findall(r"\d+",tax_date_section)
	if(len(date_list)==6):
		begin_date = date_list[0]+"-"+date_list[1]+"-"+date_list[2]
		end_date = date_list[3]+"-"+date_list[4]+"-"+date_list[5]
	else:
		print("Error when parse:"+file_path+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 246. Parse table period error.\n\n")
		return -1
	cycle_typ = report_cycle_typ(begin_date,end_date)
	
	ent_id = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td/text()')[0]
	ent_id = ent_id.split(":")[1].replace(" ","")
	ent_nm = page.xpath('//*[@id="MyDiv"]/table[2]//tr[2]/td/text()')[0]
	ent_nm = ent_nm.split("：")[1]
	
	list = page.xpath('//*[@id="MyDiv"]/table[3]//tr')
	
	detail_length = 0
	for detail in list:
		detail_length+=1
		if(detail_length<3 or detail_length >24):
			continue
		items = detail.xpath('./td')
		item_str = "insert ignore into l1_value_added_tax_small_scale values(sysdate(),'"+ent_id+"','"+begin_date+"','"+end_date+"','"+cycle_typ+"'"
		item_length = 0
		for item in items:
			item_length += 1
			if(len(items)>6 and item_length==1):
				continue
			content = item.xpath('./text()')
			if(len(content)>0):
				content = content[0]
				content = content.replace("\u3000","").replace("\xa0","")
				content = content.replace("\n","")
				if(len(items)==6 and item_length==2 and len(content)>2):
					content = re.findall(r"\d+",content)[0]
				item_str = item_str + ",'"+content+"'"
			else:
				item_str = item_str + ",''"
		item_str = item_str+");"
		mysql_execute(item_str,file_path)

def purchase_tax_analysis(file_path,ent_id):
	# print(file_path)
	# f = open(file_path,'rb') 
	# buf = f.read()
	# charRes = chardet.detect(buf)
	# f = open(file_path,'r',encoding = charRes["encoding"])
	# # f=open(file_path,'r')
	# page = etree.HTML(f.read())
	page = get_page(file_path)
	
	try:
		tax_date_section = page.xpath('//*[@id="MyDiv"]/table[1]//tr[4]/td/div/text()')[0]
	except Exception as e:
		print("Error:"+str(e)+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 292. Table error. "+str(e)+"\n\n")
		return -1
	
	date_list = re.findall(r"\d+",tax_date_section)
	if(len(date_list)==6):
		begin_date = date_list[0]+"-"+date_list[1]+"-"+date_list[2]
		end_date = date_list[3]+"-"+date_list[4]+"-"+date_list[5]
	else:
		print("Error when parse:"+file_path+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 302. Parse table period error.\n\n")
		return -1
	cycle_typ = report_cycle_typ(begin_date,end_date)
	
	# ent_id = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td[2]/text()')[0]
	ent_nm = page.xpath('//*[@id="MyDiv"]/table[1]//tr[6]/td[1]/text()')[0]
	ent_nm = ent_nm.replace(u"纳税人名称：","")
	ent_nm = ent_nm.replace(u"（公章）","")
	
	list = page.xpath('//*[@id="MyDiv"]/table[2]//tr')
	
	detail_length = 0
	for detail in list:
		detail_length+=1
		items = detail.xpath('./td')
		if(len(items)<3 or detail_length==2 or detail_length==16 or detail_length==29 or detail_length==42): # 标题行
			continue
		item_str = "insert ignore into l1_purchase_tax values(sysdate(),'"+ent_id+"','"+begin_date+"','"+end_date+"','"+cycle_typ+"'"
		item_length = 0
		for item in items:
			item_length += 1
			content = item.xpath('./text()')[0]
			content = content.replace("\u3000","").replace("\xa0","")
			content = content.replace("\n","")
			content = content.replace("──","")
			if(item_length==2 and len(content)>2):
				content = re.findall(r"\d+",content)[0]
			if(len(items)==3 and item_length==3):
				item_str = item_str + ",'',''"
			item_str = item_str + ",'"+content+"'"
		item_str = item_str+");"
		if(item_str.find(",'8a',")>0):
			detail_length-=1
			continue
		item_str = item_str.replace("'8b'","'8'")
		mysql_execute(item_str,file_path)

def purchase_detail_analysis(file_path):
	# print(file_path)
	# f = open(file_path,'rb') 
	# buf = f.read()
	# charRes = chardet.detect(buf)
	# f = open(file_path,'r',encoding = charRes["encoding"])
	# # f=open(file_path,'r')
	# page = etree.HTML(f.read())
	page = get_page(file_path)
	
	try:
		table_name = page.xpath('/html/body/div[1]/table[1]//tr[3]/td/text()')[0]
	except Exception as e:
		# print("Error:"+str(e)+"\n")
		if(str(e)!="list index out of range"):
			with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
				f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 350. Table error. "+str(e)+"\n\n")
		return -1
	if(table_name.find(u"申报抵扣明细")<0):
		try:
			table_name = page.xpath('/html/body/div[1]/table[1]//tr[3]/td/p/text()')[0]
		except Exception as e:
			with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
				f_else.write(str(datetime.now())+"\n"+file_path+"\n"+table_name+". Table error.\n\n")
			return -1
	try:
		ent_id = str(page.xpath('/html/body/div[1]/table[1]//tr[5]/td/text()')[0]).replace(" ","")
		if(len(ent_id)<10):
			ent_id = page.xpath('/html/body/div[1]/table[1]//tr[5]/td/p/text()')[0]
		deduct_period = page.xpath('/html/body/div[1]/table[1]//tr[4]/td/text()')[0]
	except Exception as e:
		print("Error:"+str(e)+"\n")
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+file_path+"\n"+"program line 364. Parse table header error. "+str(e)+"\n\n")
		return -1
	
	pattern = re.compile(r'[A-Z0-9]+')
	ent_id = pattern.findall(ent_id)[0]
	deduct_period = pattern.findall(deduct_period)
	deduct_period = "-".join(deduct_period)
	
	tables = page.xpath('//*[@class="pageEnd"]')
	table_version = 0 # 记录进项明细表格式，不同格式不同结构
	for table in tables:
		list = table.xpath('./table[2]//tr')
		detail_length = 0
		for detail in list:
			detail_length+=1
			if(detail_length==1):
				if(len(detail.xpath('./td'))==10):
					table_version = 1
					continue
				elif(len(detail.xpath('./td'))==8):
					table_version = 2
					continue
				else:
					print(file_path+"\nPurchase detail table version error!")
					exit(0)
					with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
						f_else.write(str(datetime.now())+"\n"+file_path+"\nPurchase detail table version error!\n")
			items = detail.xpath('./td')
			
			# if(table_version==1):
				# for item in items:
					# item_length += 1
					# if(detail_length==2 and item_length==1 and len(item.xpath('./text()')[0])>1):
						# continue
					# content = item.xpath('./text()')[0]
					# content = content.replace("\u3000","")
					# if(len(content)==0 and (item_length==2 or item_length==3)): # 到有数据的结尾
						# return 1
					# item_content.append(content)
				# item_str = "insert ignore into l1_purchase_detail values(sysdate(),'"+"','".join(item_content)+"');"
				# mysql_execute(item_str,file_path)
			if(table_version==1):
				i = 1 if len(items)==10 else 0
				detail_no = items[0+i].xpath('./text()')[0].replace("\u3000","").replace("-","")
				receipt_no = items[1+i].xpath('./text()')[0].replace("\u3000","")
				if(len(receipt_no)>0 and detail_no.find("计")==-1):
					receipt_id = items[2+i].xpath('./text()')[0].replace("\u3000","")
					receipt_date = items[3+i].xpath('./text()')[0].replace("\u3000","")
					amount = items[4+i].xpath('./text()')[0].replace("\u3000","")
					tax_amt = items[5+i].xpath('./text()')[0].replace("\u3000","")
					sale_ent_id = items[6+i].xpath('./text()')[0].replace("\u3000","")
					deduct_date = items[7+i].xpath('./text()')[0].replace("\u3000","")
					remarks = items[8+i].xpath('./text()')[0].replace("\u3000","")
					item_str = "insert ignore into l1_purchase_detail values(sysdate(),'"+ent_id+"','"+\
					deduct_period+"','"+detail_no+"','"+receipt_no+"','"+receipt_id+"','"+receipt_date+"','"+amount+"','"+\
					tax_amt+"','"+sale_ent_id+"','"+deduct_date+"','"+remarks+"');"
					mysql_execute(item_str,file_path)
			
			if(table_version==2):
				detail_no = items[0].xpath('./text()')[0].replace("\u3000","").replace("-","")
				receipt_no = items[1].xpath('./text()')[0].replace("\u3000","")
				if(len(receipt_no)>0 and detail_no.find("计")==-1):
					receipt_id = items[2].xpath('./text()')[0].replace("\u3000","")
					receipt_date = items[3].xpath('./text()')[0].replace("\u3000","")
					sale_ent_id = items[4].xpath('./text()')[0].replace("\u3000","")
					amount = items[5].xpath('./text()')[0].replace("\u3000","")
					tax_amt = items[6].xpath('./text()')[0].replace("\u3000","")
					invaild_flg = items[7].xpath('./text()')[0].replace("\u3000","")
					item_str = "insert ignore into l1_purchase_detail values(sysdate(),'"+ent_id+"','"+\
					deduct_period+"','"+detail_no+"','"+receipt_no+"','"+receipt_id+"','"+receipt_date+"','"+amount+"','"+\
					tax_amt+"','"+sale_ent_id+"','','"+invaild_flg+"');"
					mysql_execute(item_str,file_path)
		if(table_version==1 and len(table.xpath('./table[2]/td'))>9 and len(table.xpath('./table[2]/td'))%9==0):
			td_list = table.xpath('./table[2]/td')
			for i in range(0,len(td_list),9):
				detail_no = td_list[i].xpath('./text()')[0].replace("\u3000","").replace("-","")
				receipt_no = td_list[i+1].xpath('./text()')[0].replace("\u3000","")
				if(len(receipt_no)>0 and detail_no.find("计")==-1):
					receipt_id = td_list[i+2].xpath('./text()')[0].replace("\u3000","")
					receipt_date = td_list[i+3].xpath('./text()')[0].replace("\u3000","")
					amount = td_list[i+4].xpath('./text()')[0].replace("\u3000","")
					tax_amt = td_list[i+5].xpath('./text()')[0].replace("\u3000","")
					sale_ent_id = td_list[i+6].xpath('./text()')[0].replace("\u3000","")
					deduct_date = td_list[i+7].xpath('./text()')[0].replace("\u3000","")
					remarks = td_list[i+8].xpath('./text()')[0].replace("\u3000","")
					item_str = "insert ignore into l1_purchase_detail values(sysdate(),'"+ent_id+"','"+\
					deduct_period+"','"+detail_no+"','"+receipt_no+"','"+receipt_id+"','"+receipt_date+"','"+amount+"','"+\
					tax_amt+"','"+sale_ent_id+"','"+deduct_date+"','"+remarks+"');"
					mysql_execute(item_str,file_path)
				else:
					break

def report_cycle_typ(begin_date,end_date):
	cycle_typ = ""
	if(begin_date[:7]==end_date[:7]):
		cycle_typ = "monthly"
	elif(int(begin_date[5:7])+2==int(end_date[5:7])):
		cycle_typ = "quarterly"
	elif(begin_date[:4]==end_date[:4] and begin_date[5:7]=='01' and end_date[5:7]=='12'):
		cycle_typ = "annual"
	else:
		cycle_typ = "notknow"
	return cycle_typ

def mysql_execute(s,fPath):
	try:# 插入
		cur.execute(s)
		conn.commit()
		# print("----insert success!----")
	except pymysql.Warning as w:
		sqlWarning =  "Warning:%s" % str(w)
		# print(fPath+"\n"+sqlWarning) 
		# print(sqlWarning)
		if(sqlWarning.split(",")[0]=='Warning:(1261'):
			print(sqlWarning)
			with open(r"D:\uploadTempFiles\python\logFile\error1261.txt","a") as f1261:
				f1261.write(str(datetime.now())+"\n"+fPath+"\n"+s+"\n"+sqlWarning+"\n\n")
		elif(sqlWarning.split(",")[0]=='Warning:(1262'):
			print(sqlWarning)
			with open(r"D:\uploadTempFiles\python\logFile\error1262.txt","a") as f1262:
				f1262.write(str(datetime.now())+"\n"+fPath+"\n"+s+"\n"+sqlWarning+"\n\n")
		elif(sqlWarning.split(",")[0]=='Warning:(1265'):
			print(sqlWarning)
			with open(r"D:\uploadTempFiles\python\logFile\error1265.txt","a") as f1265:
				f1265.write(str(datetime.now())+"\n"+fPath+"\n"+s+"\n"+sqlWarning+"\n\n")
		elif(sqlWarning.split(",")[0]=='Warning:(1366'):
			if(sqlWarning!="Warning:(1366, \"Incorrect integer value: '' for column 'count' at row 1\")"):
				# with open(r"D:\uploadTempFiles\python\logFile\error1366.txt","a") as f1366:
					# f1366.write(str(datetime.now())+"\n"+fPath+"\n"+s+"\n"+sqlWarning+"\n\n")
				print(sqlWarning)
		elif(sqlWarning.split(",")[0]=='Warning:(1062'):
			pass
		elif(sqlWarning.split(",")[0]=='Warning:(3719'):
			pass
		else:
			print(sqlWarning)
			with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
				f_else.write(str(datetime.now())+"\n"+fPath+"\n"+s+"\n"+sqlWarning+"\n\n")
		conn.commit()
	except pymysql.Error as e:
		sqlError =  "Error:%s" % str(e)
		print(sqlError)
		with open(r"D:\uploadTempFiles\python\logFile\error_else.txt","a") as f_else:
			f_else.write(str(datetime.now())+"\n"+fPath+"\n"+s+"\n"+sqlError+"\n\n")
		conn.rollback()


def report_main(file_path):
	
	zipFilePath = file_path
	mPath = zipFilePath.replace(".zip","").replace(".rar","").replace(".","") # 解压缩后的路径
	
	# 解压缩
	if((zipfile.is_zipfile(zipFilePath) or rarfile.is_rarfile(zipFilePath)) and os.path.exists(mPath)):# 判断是压缩文件且已解压
		pass
	elif(zipfile.is_zipfile(zipFilePath) and os.path.getsize(zipFilePath)>22): # 有的压缩包太小无内容，无法解压。
		# print(zipFilePath)
		z = zipfile.ZipFile(zipFilePath)
		z.extractall(mPath)
	elif(rarfile.is_rarfile(zipFilePath) and os.path.getsize(zipFilePath)>22):
		z = rarfile.RarFile(zipFilePath)
		z.extractall(mPath)
	else:
		pass
	
	for rootIn, dirsIn, filesIn in os.walk(mPath):
		for nameIn in filesIn:# 处理乱码
			old_path = os.path.join(rootIn,nameIn)
			new_name = nameIn
			new_path = old_path
			try:
				new_name = nameIn.encode('cp437').decode('gbk')
				new_path = os.path.join(rootIn,new_name)
				os.rename(old_path, new_path)
			except Exception as e:
				if(str(e)[:51]!="'charmap' codec can't encode characters in position"):
					print(str(e))
				# with open(r"D:\uploadTempFiles\python\logFile\log.txt","a") as f:
					# f.write(str(e)+'\n')
	for rootIn, dirsIn, filesIn in os.walk(mPath):
		for nameIn in filesIn:
			if(nameIn.find(".all.html")>0):
				all_analysis(os.path.join(rootIn,nameIn))
	print("----- finish insert report record! -----")



def yst_main(file_path):
	
	zipFilePath = file_path
	mPath = zipFilePath.replace(".zip","").replace(".rar","").replace(".","") # 解压缩后的路径
	
	# 解压缩
	if((zipfile.is_zipfile(zipFilePath) or rarfile.is_rarfile(zipFilePath)) and os.path.exists(mPath)):# 判断是压缩文件且已解压
		shutil.rmtree(mPath)
	# 重新解压
	if(zipfile.is_zipfile(zipFilePath) and os.path.getsize(zipFilePath)>22): # 有的压缩包太小无内容，无法解压。
		# print(zipFilePath)
		z = zipfile.ZipFile(zipFilePath)
		z.extractall(mPath)
	elif(rarfile.is_rarfile(zipFilePath) and os.path.getsize(zipFilePath)>22):
		z = rarfile.RarFile(zipFilePath)
		z.extractall(mPath)
	else:
		pass
	
	filePath = ''
	filePath_mx = ''
	filePath_qd = ''
	s="select 1 from dual;"
	s_mx="select 1 from dual;"
	s_qd="select 1 from dual;"
	charset = "gbk"
	lineBreak = "\r\n"
	if(len(os.listdir(mPath))==1 and os.path.isdir(os.path.join(mPath,os.listdir(mPath)[0]))):
		mPath = os.path.join(mPath,os.listdir(mPath)[0])
		charset = "utf8"
	if('zzs_fpkj.txt' in os.listdir(mPath)):
		filePath = os.path.join(mPath,'zzs_fpkj.txt')
		if(os.path.getsize(filePath)>20):
			filePath = pymysql.escape_string(filePath)
			charset,lineBreak = file_preprocess(filePath)
			s = "load data infile \'"+filePath +"\'"+r''' ignore into table temp_l1_zzs_fpkj character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29,col30,col31,col32,col33,col34,col35,col36,col37,col38,col39,col40,col41,col42,col43,col44,col45,col46,col47,col48,col49,col50,col51,col52,col53,col54,col55,col56,col57,col58,col59,col60,col61,col62,col63,col64,col65,col66,col67,col68,col69,col70,col71,col72,col73,col74,col75) ;'''
			mysql_execute('truncate temp_l1_zzs_fpkj;','')
			mysql_execute(s,filePath)
			mysql_execute('replace into l1_zzs_fpkj select * from temp_l1_zzs_fpkj;','')
		if('jdc_fpkj.txt' in os.listdir(mPath) and os.path.getsize(os.path.join(mPath,'jdc_fpkj.txt'))>20):
			filePath = os.path.join(mPath,'jdc_fpkj.txt')
			if(os.path.getsize(filePath)>20):
				filePath = pymysql.escape_string(filePath)
				charset,lineBreak = file_preprocess(filePath)
				s = "load data infile \'"+filePath +"\'"+r''' ignore into table temp_l1_zzs_fpkj character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29,col30,col31,col32,col33,col34,col35,col36,col37,col38,col39,col40,col41,col42,col43,col44,col45,col46,col47,col48,col49,col50,col51,col52,col53,col54,col55,col56,col57,col58,col59,col60,col61,col62,col63,col64,col65,col66,col67,col68,col69,col70,col71,col72,col73,col74,col75) ;'''
				mysql_execute('truncate temp_l1_zzs_fpkj;','')
				mysql_execute(s,filePath)
				mysql_execute('replace into l1_zzs_fpkj select * from temp_l1_zzs_fpkj;','')
		if('zzs_fpkj_mx.txt' in os.listdir(mPath) and os.path.getsize(os.path.join(mPath,'zzs_fpkj_mx.txt'))>20):
			filePath_mx = os.path.join(mPath,'zzs_fpkj_mx.txt')
			filePath_mx = pymysql.escape_string(filePath_mx)
			charset,lineBreak = file_preprocess(filePath_mx)
			s_mx = "load data infile \'"+filePath_mx +"\'"+r''' ignore into table temp_l1_zzs_fpkj_mx character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29) ;'''
			mysql_execute('truncate temp_l1_zzs_fpkj_mx;','')
			mysql_execute(s_mx,filePath_mx)
			mysql_execute('replace into l1_zzs_fpkj_mx select * from temp_l1_zzs_fpkj_mx;','')
		elif('zzs_fpkj_mxjxx.txt' in os.listdir(mPath) and os.path.getsize(os.path.join(mPath,'zzs_fpkj_mxjxx.txt'))>20):
			filePath_mx = os.path.join(mPath,'zzs_fpkj_mxjxx.txt')
			filePath_mx = pymysql.escape_string(filePath_mx)
			charset,lineBreak = file_preprocess(filePath_mx)
			s_mx = "load data infile \'"+filePath_mx +"\'"+r''' ignore into table temp_l1_zzs_fpkj_mx character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29) ;'''
			mysql_execute('truncate temp_l1_zzs_fpkj_mx;','')
			mysql_execute(s_mx,filePath_mx)
			mysql_execute('replace into l1_zzs_fpkj_mx select * from temp_l1_zzs_fpkj_mx;','')
		if('zzs_fpkj_qd.txt' in os.listdir(mPath) and os.path.getsize(os.path.join(mPath,'zzs_fpkj_qd.txt'))>20):
			filePath_qd = os.path.join(mPath,'zzs_fpkj_qd.txt')
			filePath_qd = pymysql.escape_string(filePath_qd)
			charset,lineBreak = file_preprocess(filePath_qd)
			s_qd = "load data infile \'"+filePath_qd +"\'"+r''' ignore into table temp_l1_zzs_fpkj_qd character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29) ;'''
			mysql_execute('truncate temp_l1_zzs_fpkj_qd;','')
			mysql_execute(s_qd,filePath_qd)
			mysql_execute('replace into l1_zzs_fpkj_qd select * from temp_l1_zzs_fpkj_qd;','')
		elif('zzs_fpkj_qdjxx.txt' in os.listdir(mPath) and os.path.getsize(os.path.join(mPath,'zzs_fpkj_qdjxx.txt'))>20):
			filePath_qd = os.path.join(mPath,'zzs_fpkj_qdjxx.txt')
			filePath_qd = pymysql.escape_string(filePath_qd)
			charset,lineBreak = file_preprocess(filePath_qd)
			s_qd = "load data infile \'"+filePath_qd +"\'"+r''' ignore into table temp_l1_zzs_fpkj_qd character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29) ;'''
			mysql_execute('truncate temp_l1_zzs_fpkj_qd;','')
			mysql_execute(s_qd,filePath_qd)
			mysql_execute('replace into l1_zzs_fpkj_qd select * from temp_l1_zzs_fpkj_qd;','')
		mysql_execute('truncate temp_l2_receipt_detail;','')
		s_join = "replace INTO rdc.temp_l2_receipt_detail SELECT fp.col2, fp.col3, round(mx.col4,0) , CASE fp.col1 WHEN '004' THEN '1' WHEN '007' THEN '0' WHEN '026' THEN '0' ELSE '0' END, fp.col12, fp.col19, fp.col20, fp.col15 , replace(replace(replace(fp.col16, char(10), ''), char(9), ''), ' ', '') , fp.col18, fp.col17 , LEFT(from_unixtime((fp.col7 - (70 * 365 + 19)) * 24 * 60 * 60 - 8 * 60 * 60), 10) AS ncol7 , mx.col9, mx.col6, round(mx.col7,2), mx.col8 , round(mx.col6 + mx.col8, 2) , mx.tax_cd FROM ( SELECT col1, col2, col3, col7, col12 , col15, col16, col17, col18, col19 , col20 FROM temp_l1_zzs_fpkj ) fp JOIN ( SELECT col2, col3, col4, col6, col7 , col8, col9 , CASE  WHEN length(col19) >= length(col20) AND length(col19) >= length(col21) THEN col19 WHEN length(col20) > length(col19) AND length(col20) >= length(col21) THEN col20 WHEN length(col21) > length(col19) AND length(col21) > length(col20) THEN col21 ELSE col19 END AS tax_cd FROM temp_l1_zzs_fpkj_mx WHERE col9 NOT LIKE '%详见销货%' UNION ALL SELECT col2, col3, col4, col6, col7 , col8, col9 , CASE WHEN length(col19) >= length(col20) AND length(col19) >= length(col21) THEN col19 WHEN length(col20) > length(col19) AND length(col20) >= length(col21) THEN col20 WHEN length(col21) > length(col19) AND length(col21) > length(col20) THEN col21 ELSE col19 END AS tax_cd FROM temp_l1_zzs_fpkj_qd ) mx ON fp.col2 = mx.col2 AND fp.col3 = mx.col3;"
		mysql_execute(s_join,mPath)
		mysql_execute('replace into rdc.l2_receipt_detail select * from rdc.temp_l2_receipt_detail;','')
		s_sum = "replace into l3_receipt_sum select ent_tax_id, buy_ent_nm, left(receipt_date,7) as receipt_mon, sum(amount) as amount from temp_l2_receipt_detail where receipt_flg IN ('0', '1') group by ent_tax_id, buy_ent_nm, left(receipt_date,7);"
		mysql_execute(s_sum,mPath)
		
	elif('XXFP.txt' in os.listdir(mPath)):
		filePath = os.path.join(mPath,'XXFP.txt')
		if(os.path.getsize(filePath)>20):
			filePath = pymysql.escape_string(filePath)
			charset,lineBreak = file_preprocess(filePath)
			s = "load data infile \'"+filePath +"\'"+r''' ignore into table temp_l1_xxfp character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29,col30,col31,col32,col33,col34,col35,col36,col37,col38,col39,col40,col41,col42,col43,col44,col45,col46,col47,col48,col49,col50,col51,col52,col53,col54,col55,col56,col57,col58,col59,col60,col61,col62,col63,col64,col65,col66,col67,col68,col69,col70,col71,col72,col73,col74,col75,col76,col77,col78,col79,col80,col81,col82) ;'''
			mysql_execute('truncate temp_l1_xxfp;','')
			mysql_execute(s,filePath)
			mysql_execute('replace into l1_xxfp select * from temp_l1_xxfp;','')
		if('XXFP_MX.txt' in os.listdir(mPath) and os.path.getsize(os.path.join(mPath,'XXFP_MX.txt'))>20):
			filePath_mx = os.path.join(mPath,'XXFP_MX.txt')
			filePath_mx = pymysql.escape_string(filePath_mx)
			charset,lineBreak = file_preprocess(filePath_mx)
			s_mx = "load data infile \'"+filePath_mx +"\'"+r''' ignore into table temp_l1_xxfp_mx character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25) ;'''
			mysql_execute('truncate temp_l1_xxfp_mx;','')
			mysql_execute(s_mx,filePath_mx)
			mysql_execute('replace into l1_xxfp_mx select * from temp_l1_xxfp_mx;','')
		if('XXFP_XHQD.txt' in os.listdir(mPath) and os.path.getsize(os.path.join(mPath,'XXFP_XHQD.txt'))>20):
			filePath_qd = os.path.join(mPath,'XXFP_XHQD.txt')
			filePath_qd = pymysql.escape_string(filePath_qd)
			charset,lineBreak = file_preprocess(filePath_qd)
			s_qd = "load data infile \'"+filePath_qd +"\'"+r''' ignore into table temp_l1_xxfp_xhqd character set '''+charset+''' fields terminated by ',' optionally enclosed by '"' escaped by '"' lines terminated by \''''+lineBreak+'''\' (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25) ;'''
			mysql_execute('truncate temp_l1_xxfp_xhqd;','')
			mysql_execute(s_qd,filePath_qd)
			mysql_execute('replace into l1_xxfp_xhqd select * from temp_l1_xxfp_xhqd;','')
		mysql_execute('truncate temp_l2_receipt_detail;','')
		s_join = "replace INTO rdc.temp_l2_receipt_detail SELECT fp.col2, fp.col3, round(mx.col4,0) , CASE fp.col1 WHEN 's' THEN '1' WHEN 'c' THEN '0' WHEN 'p' THEN '0' ELSE '0' END , CASE fp.col35 WHEN '0' THEN '0' WHEN '1' THEN '3' ELSE '0' END, fp.col10, fp.col11, fp.col6 , replace(replace(replace(fp.col7, char(10), ''), char(9), ''), ' ', '') , fp.col9, fp.col8, LEFT(fp.col18, 10) , mx.col10, mx.col7, round(mx.col8,2), mx.col9 , round(mx.col7 + mx.col9, 2) , mx.tax_cd FROM ( SELECT col1, col2, col3, col6, col7 , col8, col9, col10, col11, col18 , col35 FROM temp_l1_xxfp ) fp JOIN ( SELECT col2, col3, col4, col7, col8 , col9, col10 , CASE  WHEN (length(col19) >= length(col20) AND length(col19) >= length(col21) AND length(col19) >= length(col22)) THEN col19 WHEN (length(col20) > length(col19) AND length(col20) >= length(col21) AND length(col20) >= length(col22)) THEN col20 WHEN (length(col21) > length(col19) AND length(col21) > length(col20) AND length(col21) >= length(col22)) THEN col21 WHEN (length(col22) > length(col19) AND length(col22) > length(col20) AND length(col22) > length(col21)) THEN col22 ELSE col19 END AS tax_cd FROM temp_l1_xxfp_mx WHERE col10 NOT LIKE '%详见销货%' UNION ALL SELECT col2, col3, col4, col7, col8 , col9, col10 , CASE  WHEN (length(col19) >= length(col20) AND length(col19) >= length(col21) AND length(col19) >= length(col22)) THEN col19 WHEN (length(col20) > length(col19) AND length(col20) >= length(col21) AND length(col20) >= length(col22)) THEN col20 WHEN (length(col21) > length(col19) AND length(col21) > length(col20) AND length(col21) >= length(col22)) THEN col21 WHEN (length(col22) > length(col19) AND length(col22) > length(col20) AND length(col22) > length(col21)) THEN col22 ELSE NULL END AS tax_cd FROM temp_l1_xxfp_xhqd ) mx ON fp.col2 = mx.col2 AND fp.col3 = mx.col3;"
		mysql_execute(s_join,mPath)
		mysql_execute('replace into rdc.l2_receipt_detail select * from rdc.temp_l2_receipt_detail;','')
		s_sum = "replace into l3_receipt_sum select ent_tax_id, buy_ent_nm, left(receipt_date,7) as receipt_mon, sum(amount) as amount from temp_l2_receipt_detail where receipt_flg IN ('0', '1') group by ent_tax_id, buy_ent_nm, left(receipt_date,7);"
		mysql_execute(s_sum,mPath)
	
	else:
		print("b")
		#continue
	
	print("---- finish yst sales record ----")

def db_work(ent_tax_id):
	date1 = date.today().strftime("%Y-%m-%d")
	year = int(date.today().strftime("%Y"))
	month = int(date.today().strftime("%m"))
	date_end = (datetime(year=year,month=month,day=1)+timedelta(days=-1)).strftime("%Y-%m")+"-01"
	date_begin = str(int(date_end[:4])-2)+date_end[4:]
	s = "SET SQL_SAFE_UPDATES = 0;"
	mysql_execute(s,"")
	s = "REPLACE INTO rdc.dim_ent_info SELECT indu.ent_tax_id, nm.ent_nm, '', '', indu.tax_cd , tax_dim1.pro_tax_nm, pro.tax_cd, tax_dim2.pro_tax_nm, 0 FROM ( SELECT s.tax_cd, s.amt, @row_number := CASE  WHEN @group_number = s.ent_tax_id THEN @row_number + 1 ELSE 1 END AS num , @group_number := s.ent_tax_id AS ent_tax_id FROM ( SELECT ent_tax_id , CASE  WHEN length(tax_code) < 2 THEN '0000000000000000000' WHEN tax_code IS NULL THEN '0000000000000000000' ELSE concat(LEFT(tax_code, 5), '00000000000000') END AS tax_cd, SUM(ttl_amount) AS amt FROM l2_receipt_detail where ent_tax_id = '"+ent_tax_id+"' and receipt_date>='"+date_begin+"' GROUP BY ent_tax_id, tax_cd ORDER BY ent_tax_id, amt DESC ) s, ( SELECT @row_number := '' ) t1, ( SELECT @group_number := '' ) t2 ) indu JOIN ( SELECT s.tax_cd, s.amt, @row_number2 := CASE  WHEN @group_number2 = s.ent_tax_id THEN @row_number2 + 1 ELSE 1 END AS num , @group_number2 := s.ent_tax_id AS ent_tax_id FROM ( SELECT ent_tax_id , CASE  WHEN length(tax_code) < 2 THEN '0000000000000000000' WHEN tax_code IS NULL THEN '0000000000000000000' ELSE concat(LEFT(tax_code, 7), '000000000000') END AS tax_cd, SUM(ttl_amount) AS amt FROM l2_receipt_detail where ent_tax_id = '"+ent_tax_id+"' and receipt_date>='"+date_begin+"' GROUP BY ent_tax_id, tax_cd ORDER BY ent_tax_id, amt DESC ) s, ( SELECT @row_number2 := '' ) t1, ( SELECT @group_number2 := '' ) t2 ) pro ON indu.ent_tax_id = pro.ent_tax_id AND indu.num = pro.num JOIN ( SELECT ent_tax_id, MAX(ent_nm) AS ent_nm FROM l2_receipt_detail where ent_tax_id = '"+ent_tax_id+"' and receipt_date>='"+date_begin+"' GROUP BY ent_tax_id ) nm ON indu.ent_tax_id = nm.ent_tax_id LEFT JOIN dim_product_tax_code_level tax_dim1 ON indu.tax_cd = tax_dim1.pro_tax_cd LEFT JOIN dim_product_tax_code_level tax_dim2 ON pro.tax_cd = tax_dim2.pro_tax_cd WHERE indu.num = 1 AND pro.num = 1;"
	mysql_execute(s,"")
	s = "UPDATE dim_ent_info ent SET indu_ent_cnt = ( SELECT b.cnt FROM ( SELECT a.indu_cd , round(198765 * a.cnt / b.all_cnt, 0) AS cnt FROM ( SELECT indu_cd, COUNT(*) AS cnt FROM dim_ent_info GROUP BY indu_cd ) a JOIN ( SELECT COUNT(*) AS all_cnt FROM dim_ent_info ) b ON 1 = 1 ) b WHERE ent.indu_cd = b.indu_cd )"
	mysql_execute(s,"")
	print("---- finish database work ----")


def once_input_main(ent_tax_id, data_type):
	if(data_type=='xx'):
		if(len(ent_tax_id)<15): # 测试数据，可能存在税号长度过短，跳过。
			return 0
		print("input sales data......")
		ent_list_path = "D:/uploadTempFiles/DATA/"+date.today().strftime("%Y%m%d")+"/"+ent_tax_id[:2]
		zip_name = ''
		file_path = ''
		find_flag = 0
		res = 0 #1为正常导入，0为未找到数据 find的函数 找到的返回的是0，没有找到的返回的是-1
		for name in os.listdir(ent_list_path):
			if(name.find(ent_tax_id)>=0 and os.path.isfile(os.path.join(ent_list_path,name))):
				zip_name = name
				find_flag = 1
				file_path = os.path.join(ent_list_path,zip_name)
				yst_main(file_path)
				db_work(ent_tax_id)
				res = 1
		if(find_flag==0):
			res = 0
		return res
	elif(data_type == 'sb'):
		print("input tax report data......")
		ent_list_path = "D:/uploadTempFiles/shenbao/DATA/"+date.today().strftime("%Y%m%d")+"/"+ent_tax_id[:2]
		zip_name = ''
		file_path = ''
		find_flag = 0
		res = 0 #1为正常导入，0为未找到数据
		for name in os.listdir(ent_list_path):
			if(name.find(ent_tax_id)>=0 and os.path.isfile(os.path.join(ent_list_path,name))):
				zip_name = name
				find_flag =1
				file_path = os.path.join(ent_list_path,zip_name)
				report_main(file_path)
				res = 1
		if(find_flag==0):
			res = 0
		return res
	else:
		print("输入有误")

if __name__=="__main__":
	ent_tax_id = sys.argv[1]
	data_type = sys.argv[2]
	once_input_main(ent_tax_id, data_type)
