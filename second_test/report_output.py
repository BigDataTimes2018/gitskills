import zlib
import os
import zipfile
import rarfile
import pymysql
import traceback
import time
from lxml import etree
import re
import sys
from warnings import filterwarnings
from datetime import datetime,date,timedelta
import uuid
from once_input import once_input_main
from toReport import toReport_main
from aes_encrypt import aes_encrypt
filterwarnings('error', category = pymysql.Warning)

# 本程序分为part1和part2，根据参数1为'1'或'2'来区分。
# part1负责查询库中未处理企业，并启动导入销项数据程序。导入后判断是否断档，断档则拒绝。输出销项数据正常导入企业列表。
# part2需在申报数据获取完毕时调用。调用时，参数2为part1输出的销项数据正常导入列表。
# 完成申报数据导入、输出报告、判断分数选择拒绝或推送、更新报告记录表的工作。

conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='yst@2019',db='yst')
cur = conn.cursor()

def executeSQL(SQL):
	SQL = SQL.replace('\\','\\\\')
	try:# 插入
		cur.execute(SQL)
		conn.commit()
		# print("----insert success!----")
	except pymysql.Warning as w:
		sqlWarning =  "Warning:%s" % str(w)
		print(sqlWarning)
	except pymysql.Error as e:
		sqlError =  "Error:%s" % str(e)
		print(sqlError)

def generate_uuid():
	id = uuid.uuid1()
	id = str(id)
	id = id.replace("-","")
	return id

def report_output_part1(): # 导入销项数据
	# 日期参数，用于判断正确的申报日期区间，或计算断档使用日期区间
	year = int(date.today().strftime("%Y"))
	month = int(date.today().strftime("%m"))
	begin1 = (datetime(year=year,month=month,day=1)+timedelta(days=-1)).strftime("%Y-%m")+"-01" # 上月1号
	end1 = (datetime.strptime(begin1,"%Y-%m-%d")+timedelta(days=-1)).strftime("%Y-%m-%d") #上上月末
	begin2 = (datetime(year=year,month=month,day=1)).strftime("%Y-%m-%d") # 本月1号
	end2 = (datetime.strptime(begin2,"%Y-%m-%d")+timedelta(days=-1)).strftime("%Y-%m-%d") #上月末
	ent_ok = [] # 正常企业
	ent_dd = [] #断档企业
	ent_noreceipt = [] #正常导入数据包但无开票记录企业
	ent_nodata = [] # 无数据企业
	ent_noaddress = [] # 无地址的企业
	try:
        #核验数据成功，待生成推送报告
		s = "select seria_id, ent_tax_id from yst.loan_info where apply_status='2' or apply_status is null"
		# s = "select distinct ent_tax_id from register_auth_info where authenticate_status=1 and report_output_flag = 0"
		cur.execute(s)
		ent_list = cur.fetchall()
		if(len(ent_list)>0):
			for ent in ent_list:
				apply_id = ent[0]
				ent_tax_id = ent[1]
				# ent_tax_id = ent[0]
				print(ent_tax_id +" 处理中……")
				# 导入销项数据
				res = once_input_main(ent_tax_id,'xx') #1为正常导入，0为未找到数据
				if(res==1):
					# 判断断档
					conn2 = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='yst@2019',db='rdc')
					cur2 = conn.cursor()
					s1 = "select date_format(min(receipt_date),\"%Y-%m-%d\"), date_format(max(receipt_date),\"%Y-%m-%d\") \
					from rdc.l2_receipt_detail where ent_tax_id = '" + ent_tax_id+"'"
					cur2.execute(s1)
					res1 = cur2.fetchall()
					# left函数 取出一个字符串的若干位
					s2 = "select count(distinct left(receipt_date,7)) from rdc.l2_receipt_detail where ent_tax_id = '" \
					+ ent_tax_id +"' and receipt_date>= date_add('"+begin2+"', INTERVAL -1 YEAR) and receipt_date<'"+begin2+"' "
					cur2.execute(s2)
					res2 = cur2.fetchall()
					cur2.close()
					conn2.close()
					if(res1[0][0]==None):
						ent_noreceipt.append(ent_tax_id)
						print("销项数据为空或导入失败")
						continue
					print("销项数据覆盖时间区间："+" ".join(res1[0]))
					print("近12个月有销项数据月份数："+str(res2[0][0]))
					if(res2[0][0]<10): # 断档严重
						insertSQL = "insert into yst.report_refuse_record select current_timestamp(),ent_tax_id, ent_nm\
						, '断档' from rdc.dim_ent_info where ent_tax_id = '"+ent_tax_id+"';"
						executeSQL(insertSQL)
						tempSQL = "SET SQL_SAFE_UPDATES = 0;"
						executeSQL(tempSQL)
						# updateSQL = "update yst.register_auth_info set report_output_flag = -1 where ent_tax_id ='"+ent_tax_id+"';"
						# executeSQL(updateSQL)
                        # 状态为3的企业，代表是企业开票断档情况比较严重的企业
						updateSQL = "update yst.loan_info set apply_status = '3', apply_status_memo = '未通过审核，纳税断档（增值税开票断档）超过要求。' where seria_id ='"+apply_id+"';"
						executeSQL(updateSQL)
                        # 3表示断档严重，从2转成3，1表示的是数据校验的时候
						insertSQL = "insert into yst.loan_apply_oper_record values('"+generate_uuid()+"','3','"+apply_id+"','2',\
						'report_output.py',current_timestamp(),'1','纳税断档（增值税开票断档）超过要求');"
						executeSQL(insertSQL)
						ent_dd.append(ent_tax_id)
						print("断档严重并拒绝。")
					else:
						ent_ok.append(ent_tax_id)
						s = "select ent_real_address, loan_amt from loan_info where seria_id ='"+apply_id+"';"
						cur.execute(s)
						res = cur.fetchall()
						if(res[0][0]==None or res[0][1]==None):
							print("该企业缺失企业地址或申贷金额")
							ent_noaddress.append(ent_tax_id)
				else:
					ent_nodata.append(ent_tax_id)
					print("未找到销项数据包")
			print("今日待处理企业：\n"+",".join(ent[1] for ent in ent_list))
			# print("今日待处理企业：\n"+",".join(ent[0] for ent in ent_list))
			if(len(ent_dd)>0):
				print("其中，断档已拒绝企业：\n"+",".join(ent_dd))
			if(len(ent_nodata)>0):
				print("未找到数据包企业：\n"+",".join(ent_nodata))
			if(len(ent_noreceipt)>0):
				print("找到数据包，但无数据或导入失败企业：\n"+",".join(ent_noreceipt))
			if(len(ent_ok)>0):
				print("销项正常，待获取申报数据企业(List for enterprises preparing tax reports)：\n"+",".join(ent_ok))
			if(len(ent_noaddress)>0):
				print("销项正常，但无企业地址或申贷金额企业(List for enterprises without address)：\n"+",".join(ent_noaddress))
		else:
			print("无未输出报告企业。")
		
	except Exception as e:
		with open(r"D:\uploadTempFiles\python\log.txt","a") as f:
			f.write(str(e)+'\n')

def TimeStampToTime(timestamp):
	timeStruct = time.localtime(timestamp)
	return time.strftime('%Y-%m-%d %H:%M:%S',timeStruct)

def report_process(ent_tax_id,begin_date): #返回0，1。0表示拒绝，1表示推送
	bank_nm = 'guandu'
	# enterExcel(ent_tax_id,begin_date)
	ent_nm,score = toReport_main(ent_tax_id, begin_date)
	
	# 更新loan_report_manage_record表
	file_doc = os.path.join("D:\\report\\ori\\daiqian",ent_nm +"_"+ date.today().strftime("%Y%m%d")+"_定量分析.doc")
	file_xls = os.path.join("D:\\report\\ori\\daiqian",ent_nm +"_"+ date.today().strftime("%Y%m%d")+"_定量评分.xls")
	ori_file_size_doc = str(os.path.getsize(file_doc))
	ori_file_create_time_doc = TimeStampToTime(os.path.getmtime(file_doc))
	ori_file_size_xls = str(os.path.getsize(file_xls))
	ori_file_create_time_xls = TimeStampToTime(os.path.getmtime(file_xls))
	
	sql = "select seria_id, product_id from yst.loan_info where ent_tax_id = '"+ent_tax_id+"' \
	and (apply_status='2' or apply_status is null);"
	cur.execute(sql)
	apply_info = cur.fetchall() 
	apply_info = apply_info[0]
	apply_id = apply_info[0]
	product_id = apply_info[1]
	
	s = "replace into loan_report_manage_record values('"+apply_id+"','"+ent_nm+"','100001','"+product_id+"','1','"+\
	file_doc+"','doc','"+str(ori_file_size_doc)+"','"+ori_file_create_time_doc+"','"+"',null,null,null,null,null);"
	executeSQL(s)
	s = "replace into loan_report_manage_record values('"+apply_id+"','"+ent_nm+"','100001','"+product_id+"','0','"+\
	file_xls+"','xls','"+str(ori_file_size_xls)+"','"+ori_file_create_time_xls+"','"+"',null,null,null,null,null);"
	executeSQL(s)
	
	if(score>40):
		aes_encrypt(ent_nm,bank_nm)
		insertSQL = "insert into yst.report_push_record(ent_tax_id,ent_nm,bank_nm) values('"+ent_tax_id+"','"+ent_nm+"','"+bank_nm+"');"
		executeSQL(insertSQL)
		tempSQL = "SET SQL_SAFE_UPDATES = 0;"
		executeSQL(tempSQL)
		# updateSQL = "update register_auth_info set report_output_flag = 1 where ent_tax_id ='"+ent_tax_id+"';"
		# executeSQL(updateSQL)
        # 状态为6表示的报告推送成功
		updateSQL = "update yst.loan_info set apply_status = '6', apply_status_memo = '报告推送成功。' where seria_id ='"+apply_id+"';"
		executeSQL(updateSQL)
		insertSQL = "insert into yst.loan_apply_oper_record values('"+generate_uuid()+"','6','"+apply_id+"','2','report_output.py'\
		,current_timestamp(),'3','');"
		executeSQL(insertSQL)
		
		# 更新loan_report_manage_record表
		dirpath = "D:\\report\\"+bank_nm+"\\daiqian\\"+date.today().strftime("%Y-%m-%d")+"\\"+ent_nm
		enc_file_doc = os.path.join(dirpath,file_doc.split("\\")[-1].replace(".doc",".doc.enc"))
		enc_file_xls = os.path.join(dirpath,file_xls.split("\\")[-1].replace(".xls",".xls.enc"))
		enc_file_size_doc = str(os.path.getsize(enc_file_doc))
		enc_file_create_time_doc = TimeStampToTime(os.path.getmtime(enc_file_doc))
		enc_file_size_xls = str(os.path.getsize(enc_file_xls))
		enc_file_create_time_xls = TimeStampToTime(os.path.getmtime(enc_file_xls))
		
		s = "update loan_report_manage_record set dest_file_path = '"+enc_file_doc+"', dest_file_size = '"\
		+enc_file_size_doc+"', push_time = '"+enc_file_create_time_doc+"' where apply_id = '"+apply_id+"' and report_type = '1';"
		executeSQL(s)
		s = "update loan_report_manage_record set dest_file_path = '"+enc_file_xls+"', dest_file_size = '"\
		+enc_file_size_xls+"', push_time = '"+enc_file_create_time_xls+"' where apply_id = '"+apply_id+"' and report_type = '0';"
		executeSQL(s)
		
		print(ent_nm+"  分数："+str(score)+"。已推送!")
		return 1
	else:
		insertSQL = "insert into yst.report_refuse_record values(current_timestamp(),'"+ent_tax_id+"','"+ent_nm+"','分低');"
		executeSQL(insertSQL)
		tempSQL = "SET SQL_SAFE_UPDATES = 0;"
		executeSQL(tempSQL)
		# updateSQL = "update yst.register_auth_info set report_output_flag = -1 where ent_tax_id ='"+ent_tax_id+"';"
		# executeSQL(updateSQL)
		updateSQL = "update yst.loan_info set apply_status = '14' , apply_status_memo = '未通过审核，税控数据评分未达标。' where seria_id ='"+apply_id+"';"
		executeSQL(updateSQL)
		insertSQL = "insert into yst.loan_apply_oper_record values('"+generate_uuid()+"','14','"+apply_id+"','2','report_output.py'\
		,current_timestamp(),'11','评分低于产品标准拒绝');"
		executeSQL(insertSQL)
		print(ent_nm+"  分数："+str(score)+"。分数过低已拒绝!")
		return 0

def report_output_part2(ent_str):
	# 日期参数，用于判断正确的申报日期区间，或计算断档使用日期区间
	year = int(date.today().strftime("%Y"))
	month = int(date.today().strftime("%m"))
	begin1 = (datetime(year=year,month=month,day=1)+timedelta(days=-1)).strftime("%Y-%m")+"-01" # 上月1号
	end1 = (datetime.strptime(begin1,"%Y-%m-%d")+timedelta(days=-1)).strftime("%Y-%m-%d") #上上月末
	begin2 = (datetime(year=year,month=month,day=1)).strftime("%Y-%m-%d") # 本月1号
	end2 = (datetime.strptime(begin2,"%Y-%m-%d")+timedelta(days=-1)).strftime("%Y-%m-%d") #上月末
	ent_list = ent_str.split(",")
	
	ent_ok = [] # 正常企业
	ent_dd = [] #申报缺失企业
	ent_nodata = [] # 无数据企业
	ent_push = [] #已推送企业
	
	for ent_tax_id in ent_list:
		print(ent_tax_id +" 处理中……")
		# 状态为2表示的是数据核验成功，可以进行出报告
		sql = "select seria_id from yst.loan_info where ent_tax_id = '"+ent_tax_id+"' and (apply_status='2' or apply_status is null);"
		cur.execute(sql)
		apply_info = cur.fetchall() 
		apply_id = apply_info[0][0]
		print("*************************")
		print(apply_id)
		print("((((((((((((((((((((((((")
		# 导入申报数据
		res = once_input_main(ent_tax_id,'sb') #1为正常，0为未找到数据
		if(res==1):
			#判断申报数据
			s3 = "select date_format(max(end_date),\"%Y-%m-%d\"), date_format(min(end_date),\"%Y-%m-%d\"),b.item_nm from rdc.l1_all_list a \
			left join (select item_nm from rdc.l1_all_list where ent_tax_id = '"+ent_tax_id+"' order by end_date desc limit 1) b on 1=1 \
			where ent_tax_id = '"+ent_tax_id+"' and a.item_nm like '%增值税纳税申报表%'"
			conn3 = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='yst@2019',db='rdc')
			cur3 = conn.cursor()
			cur3.execute(s3)
			res3 = cur3.fetchall()
			cur3.close()
			conn3.close()
			print("申报数据覆盖时间区间："+",".join(res3[0]))
			
			rpt_res = 0 #记录报告结果
			# end1=上上个月末 end2是上个月末 00取出的是最大的时间
			# begin1是上月1号，begin2是本月1号
			if(res3[0][0]==end1):
				rpt_res = report_process(ent_tax_id, begin1)
				ent_ok.append(ent_tax_id)
			elif(res3[0][0]==end2):
				rpt_res = report_process(ent_tax_id,begin2)
				ent_ok.append(ent_tax_id)
			elif(res3[0][2].find("小规模纳税人适用")>=0):
				# 小微企业，要考虑到按季提交报表的情况。如7月10日时小微企业报表截止到3月31日是正常的。
				# 判断时间差，105表示一个季度加半个月。
				if((datetime.today().date() - datetime.strptime(res3[0][0],"%Y-%m-%d").date()).days<=105):
					rpt_res = report_process(ent_tax_id,begin2)
				else:
					ent_dd.append(ent_tax_id)
					insertSQL = "insert into yst.report_refuse_record select current_timestamp(),ent_tax_id, ent_nm\
					, '申报数据缺失' from rdc.dim_ent_info where ent_tax_id = '"+ent_tax_id+"';"
					executeSQL(insertSQL)
					tempSQL = "SET SQL_SAFE_UPDATES = 0;"
					executeSQL(tempSQL)
					updateSQL = "update yst.loan_info set apply_status = '4', apply_status_memo = '未通过审核，税控数据评分未达标。' where seria_id ='"+apply_id+"';"
					executeSQL(updateSQL)
					insertSQL = "insert into yst.loan_apply_oper_record values('"+generate_uuid()+"','4','"+apply_id+"','2',\
					'report_output.py',current_timestamp(),'1','评分低于产品标准拒绝。');"
					executeSQL(insertSQL)
					ent_dd.append(ent_tax_id)
					print("申报数据缺失，拒绝。")
				ent_ok.append(ent_tax_id)
			else:
				ent_dd.append(ent_tax_id)
				insertSQL = "insert into yst.report_refuse_record select current_timestamp(),ent_tax_id, ent_nm\
				, '申报数据缺失' from rdc.dim_ent_info where ent_tax_id = '"+ent_tax_id+"';"
				executeSQL(insertSQL)
				tempSQL = "SET SQL_SAFE_UPDATES = 0;"
				executeSQL(tempSQL)
				updateSQL = "update yst.loan_info set apply_status = '4', apply_status_memo = '未通过审核，税控数据评分未达标。' where seria_id ='"+apply_id+"';"
				executeSQL(updateSQL)
				insertSQL = "insert into yst.loan_apply_oper_record values('"+generate_uuid()+"','4','"+apply_id+"','2',\
				'report_output.py',current_timestamp(),'1','评分低于产品标准拒绝。');"
				executeSQL(insertSQL)
				ent_dd.append(ent_tax_id)
				print("申报数据缺失，拒绝。")
			
			if(rpt_res==1):
				ent_push.append(ent_tax_id)
		else:
			ent_nodata.append(ent_tax_id)
			print("未找到申报数据包")
	print("今日待处理企业：\n"+",".join(ent for ent in ent_list))
	print("其中，申报数据未更新到最新月份企业：\n"+",".join(ent_dd))
	print("未找到申报数据包企业：\n"+",".join(ent_nodata))
	print("成功导入申报数据企业：\n"+",".join(ent_ok))
	print("其中，已推送企业：\n"+",".join(ent_push))

if __name__=="__main__":
	part = sys.argv[1]
	if(part=='1'):
		report_output_part1()
	if(part=='2'):
		report_output_part2(sys.argv[2])