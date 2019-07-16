import re
import os
import pymysql
import traceback
import shutil
from datetime import datetime,date,timedelta
from warnings import filterwarnings
filterwarnings('error', category = pymysql.Warning)

conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='',db='yst')
cur = conn.cursor()

def mysql_execute(s):
	try:# 插入
		cur.execute(s)
		conn.commit()
		# print("----insert success!----")
	except pymysql.Warning as w:
		sqlWarning =  "Warning:%s" % str(w)
		print(sqlWarning)
		with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
			f.write(str("----WARNING----\n"+s+"\n"+sqlWarning+"\n\n"))
	except pymysql.Error as e:
		sqlError =  "Error:%s" % str(e)
		print(sqlError)
		with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
			f.write(str("----ERROR----\n"+s+"\n"+sqlError+"\n\n"))


def log_analysis_sales(filepath):
	# 一个日志set，以“[服务器时间:”开始，以“---------End”结束
	# 记录每个set的服务器时间

	file = open(filepath,'r')

	try:
		alllines = file.readlines()
	except UnicodeDecodeError as e:
		sqlError =  "Error:%s" % str(e)
		with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
			f.write(filepath+"\n"+str("----file read error----\n"+sqlError+"\n\n"))
		return -1

	setflag = 0 #标记是否在一个日志set内
	ent_tax_id = "" #记录企业税号
	visit_time = "" #记录日志set开始时间
	visit_status = "" #记录访问状态
	error_des = "" #校验失败原因描述
	for i in range(len(alllines)):
		if(alllines[i].find("[服务器时间:")>=0):
			setflag = 1
			visit_time = re.findall(r'\[[\u4e00-\u9fa5]{5}:[0-9\-]+\s[0-9:\s]+\]',alllines[i])[0].replace("[服务器时间:","")[:-5]
		if(alllines[i].find("---------End")>=0):
			setflag = 0
			tax_id_match_res = re.findall(r'End-+[0-9a-zA-Z]+[.]{0,1}[0-9]{0,1}-+End',alllines[i])
			if(len(tax_id_match_res)>0):
				ent_tax_id = tax_id_match_res[0].replace("End","").replace("-","").split(".")[0]
			else:
				continue
			if(i>0):
				match_res = re.findall(r'[\u4e00-\u9fa5，。！ =]{4,}',alllines[i-1]) #匹配中文描述
				if(len(match_res)==0):
					with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
						f.write(str(filepath+"\n----unknow situation!----\n\n"))
				elif(match_res[-1]=="，成功校验"):
					visit_status = "成功校验"
				elif(match_res[-1]=="成功校验"):
					visit_status = "成功校验"
				elif(match_res[-1]=="，成功采集"):# 较早期使用的是采集字样
					visit_status = "成功采集"
				elif(match_res[-1]=="，校验失败"):
					visit_status = "校验失败"
					if(i>1):
						error_des = alllines[i-2]
						error_des = pymysql.escape_string(error_des)
				elif(match_res[-1]=="，采集失败"):
					visit_status = "采集失败"
					if(i>1):
						error_des = alllines[i-2]
						error_des = pymysql.escape_string(error_des)
				elif(match_res[-1]=="，备份失败"):
					visit_status = "备份失败"
					if(i>1):
						error_des = alllines[i-2]
						error_des = pymysql.escape_string(error_des)
				elif(match_res[-1]=="已经成功校验，不再重复校验"):
					visit_status = "已经成功校验，不再重复校验"
				elif(match_res[-1]=="已经成功采集，不再重复采集"):
					visit_status = "已经成功采集，不再重复采集"
				elif(match_res[-1]=="服务器拒绝校验，暂缓校验。"):
					visit_status = "服务器拒绝校验，暂缓校验。"
				elif(match_res[-1]=="服务器拒绝采集，暂缓采集。"):
					visit_status = "服务器拒绝采集，暂缓采集。"
				elif(alllines[i-1].find("结束校验税务目录")>=0):
					visit_status = "结束校验"
				elif(alllines[i-1].find("结束采集税务目录")>=0):
					visit_status = "结束采集"
				else:
					with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
						f.write(str(filepath+"\n----unknow situation!----\n"+alllines[i-1]+"\n\n"))
					visit_status = match_res[-1]
			print("结束一次日志set\n")
			print("企业税号："+ent_tax_id+" 访问时间："+visit_time+" 访问状态："+visit_status+error_des+"\n")
			item_str = "insert ignore into log_analysis_sales values('"+ent_tax_id+"','"+visit_time+"','"+visit_status+"','"+error_des+"');"
			mysql_execute(item_str)


def log_analysis_shenbao(filepath):
	# 一个日志set，以“[服务器时间:”开始，以“---------End”结束
	# 记录每个set的服务器时间

	file = open(filepath,'r')

	try:
		alllines = file.readlines()
	except UnicodeDecodeError as e:
		sqlError =  "Error:%s" % str(e)
		with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
			f.write(filepath+"\n"+str("----file read error----\n"+sqlError+"\n\n"))
		return -1

	setflag = 0 #标记是否在一个日志set内
	ent_tax_id = "" #记录企业税号
	visit_time = "" #记录日志set开始时间
	visit_status = "" #记录访问状态
	error_des = "" #校验失败原因描述
	for i in range(len(alllines)):
		if(alllines[i].find("[服务器时间:")>=0):
			setflag = 1
			visit_time = re.findall(r'\[[\u4e00-\u9fa5]{5}:[0-9\-]+\s[0-9:\s]+\]',alllines[i])[0].replace("[服务器时间:","")[:-5]
		if(alllines[i].find("---------End")>=0):
			setflag = 0
			tax_id_match_res = re.findall(r'End-+[0-9a-zA-Z]+[.]{0,1}[0-9]{0,1}-+End',alllines[i])
			if(len(tax_id_match_res)>0):
				ent_tax_id = tax_id_match_res[0].replace("End","").replace("-","").split(".")[0]
			else:
				continue
			if(i>0):
				match_res = re.findall(r'[\u4e00-\u9fa5，。！ =]{4,}',alllines[i-1]) #匹配中文描述
				if(len(match_res)==0):
					with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
						f.write(str(filepath+"\n----unknow situation!----\n\n"))
				elif(match_res[-1]=="，申报数据校验成功"):
					visit_status = "申报数据校验成功"
				elif(match_res[-1]=="，申报数据备份成功"):
					visit_status = "申报数据备份成功"
				elif(match_res[-1]=="成功校验"):
					visit_status = "成功校验"
				elif(match_res[-1]=="，申报数据校验失败"):
					visit_status = "申报数据校验失败"
					if(i>1):
						error_des = alllines[i-2][20:]
						error_des = pymysql.escape_string(error_des)
				elif(match_res[-1]=="，校验失败"):
					visit_status = "校验失败"
					if(i>1):
						error_des = alllines[i-2]
						error_des = pymysql.escape_string(error_des)
				elif(match_res[-1]=="，申报数据备份失败"):
					visit_status = "申报数据备份失败"
					if(i>1):
						error_des = alllines[i-2][20:]
						error_des = pymysql.escape_string(error_des)
				elif(match_res[-1]=="已经成功校验申报数据，不再重复校验"):
					visit_status = "已经成功校验申报数据，不再重复校验"
				elif(match_res[-1]=="本地配置拒绝备份申报数据，暂缓备份。"):
					visit_status = "本地配置拒绝备份申报数据，暂缓备份。"
				elif(match_res[-1]=="服务器拒绝校验，暂缓校验。"):
					visit_status = "服务器拒绝校验，暂缓校验。"
				elif(match_res[-1]=="服务器拒绝校验申报数据，暂缓校验。"):
					visit_status = "服务器拒绝校验申报数据，暂缓校验。"
				elif(match_res[-1]=="服务器拒绝校验，暂缓校验。"):
					visit_status = "服务器拒绝校验，暂缓校验。"
				elif(match_res[-1]=="服务器拒绝备份申报数据，暂缓备份。"):
					visit_status = "服务器拒绝备份申报数据，暂缓备份。"
				elif(alllines[i-1].find("结束校验税务目录")>=0):
					visit_status = "结束校验"
				elif(alllines[i-1].find("结束备份税务目录")>=0):
					visit_status = "结束备份"
				else:
					with open(r"D:\uploadTempFiles\python\logFile\error_log_analysis.txt","a") as f:
						f.write(str(filepath+"\n----unknow situation!----\n"+alllines[i-1]+"\n\n"))
					visit_status = match_res[-1]
			print("结束一次日志set\n")
			print("企业税号："+ent_tax_id+" 访问时间："+visit_time+" 访问状态："+visit_status+error_des+"\n")
			item_str = "insert ignore into log_analysis_shenbao values('"+ent_tax_id+"','"+visit_time+"','"+visit_status+"','"+error_des+"');"
			mysql_execute(item_str)


def log_analysis_main():
	
	print("----  "+str(datetime.now())+" log analysis begin!  ----\n")
	with open(r"D:\uploadTempFiles\python\logFile\log.txt","a") as f:
		f.write("----  "+str(datetime.now())+" log analysis begin!  ----\n")
	
	yesterday = (date.today() + timedelta(days = -1)).strftime("%Y%m%d")
	# yesterday = "20190411"
	
	#销项日志
	dir = r"D:\uploadTempFiles\LOGS"
	for dirName in os.listdir(dir):
		if(len(dirName)==8 and dirName[:2]=="20" and dirName != date.today().strftime("%Y%m%d")):
			if not os.path.exists(os.path.join(dir,dirName[:6])):
				os.makedirs(os.path.join(dir,dirName[:6]))
			shutil.move(os.path.join(dir,dirName),os.path.join(dir,dirName[:6]))
		
	for root, dirs, files in os.walk(dir):
		dir_date = re.findall(r'20[0-9]{6}',root)
		if(len(dir_date)==0):# logs
			continue
		elif(root[-8:]==dir_date[0]): # 最后八位是日期
			if(dir_date[0]==date.today().strftime("%Y%m%d") or dir_date[0]<yesterday):
				print("----pass "+dir_date[0]+"----")
		else:
			if(dir_date[0]!=date.today().strftime("%Y%m%d") and dir_date[0]>=yesterday):
				for file in files:
					print(file)
					log_analysis_sales(os.path.join(root,file))

	#申报日志
	dir = r"D:\uploadTempFiles\shenbao\LOGS"
	for dirName in os.listdir(dir):
		if(len(dirName)==8 and dirName[:2]=="20" and dirName != date.today().strftime("%Y%m%d")):
			if not os.path.exists(os.path.join(dir,dirName[:6])):
				os.makedirs(os.path.join(dir,dirName[:6]))
			shutil.move(os.path.join(dir,dirName),os.path.join(dir,dirName[:6]))
		
	for root, dirs, files in os.walk(dir):
		dir_date = re.findall(r'20[0-9]{6}',root)
		if(len(dir_date)==0):# logs
			continue
		elif(root[-8:]==dir_date[0]): # 最后八位是日期
			if(dir_date[0]==date.today().strftime("%Y%m%d") or dir_date[0]<yesterday):
				print("----pass "+dir_date[0]+"----")
		else:
			if(dir_date[0]!=date.today().strftime("%Y%m%d") and dir_date[0]>=yesterday):
				for file in files:
					log_analysis_shenbao(os.path.join(root,file))


if __name__=="__main__":
	log_analysis_main()