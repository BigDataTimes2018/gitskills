import base64
from Crypto.Cipher import AES
import hashlib
import os
import sys
from datetime import datetime,date
import pymysql
from warnings import filterwarnings
filterwarnings('error', category = pymysql.Warning)

def bytePad(text,byteAlignLen=16):
	count=len(text)
	mod_num=count%byteAlignLen
	if mod_num==0:
		return text
	add_num=byteAlignLen-mod_num
	print("bytePad:",add_num)
	text2 = chr(add_num)*add_num
	return text+text2.encode(encoding='utf-8')

def aes_encrypt(file_name,bank_nm,rpt_type):
	
	ent_nm = file_name.split("\\")[-1].split("_")[0]
	
	# md5加密
	m = hashlib.md5()
	str = ent_nm+bank_nm+"mP3s#&s" # 加密key为企业名称+银行名+随机码
	b = str.encode(encoding='utf-8')
	m.update(b)
	key = m.hexdigest()
	# key = key + "Q823#MS9$~01"
	key = key.encode(encoding="utf-8")
	key = key[:16]

	# 文件内容用md5加密，并写进检核文件。
	f =open(file_name,mode='rb')
	text_f = f.read()
	m.update(text_f)
	md5_text_f = m.hexdigest() #写检核文件用
	f.close()

	aes = AES.new(key, AES.MODE_ECB) # 初始化aes加密器

	# 文件内容用aes加密处理
	encrypted_text = base64.b64encode(aes.encrypt(bytePad(text_f))).decode().replace('\n', '')
	# print(encrypted_text)

	# 新建日期文件夹
	datedirpath = "D:/report/"+bank_nm+"/"+rpt_type+"/"+date.today().strftime("%Y-%m-%d")
	if not os.path.exists(datedirpath):
		os.makedirs(datedirpath)
		f_blank = open(datedirpath+"/"+date.today().strftime("%Y-%m-%d")+"_OK.txt","w")
		f_blank.close()

	# 新建企业名文件夹
	dirpath = "D:/report/"+bank_nm+"/"+rpt_type+"/"+date.today().strftime("%Y-%m-%d")+"/"+ent_nm
	if(rpt_type == 'daihou'):
		dirpath = dirpath+"_贷后"
	if not os.path.exists(dirpath):
		os.makedirs(dirpath)

	# aes加密后内容写入加密文件
	f_encrypt = open(dirpath+"/"+file_name.split("\\")[-1]+".enc",'w')
	f_encrypt.write(encrypted_text)
	f_encrypt.close()

	# 写检核文件
	check_path = ""
	check_path = dirpath+"/"+ent_nm+"_检核.ok"
	f_check = open(check_path,'w')

	f_encrypt = open(dirpath+"/"+file_name.split("\\")[-1]+".enc",'rb')
	f_encrypt_text = f_encrypt.read()
	m.update(f_encrypt_text)
	f_check.write(dirpath+"/"+file_name.split("\\")[-1]+".enc"+'\t'+md5_text_f+'\t'+m.hexdigest()+'\n')
	f_encrypt.close()

	f_check.close()

def daiqian_report_encrypt(ent_nm,bank_nm):
	dir = r"D:\report\ori\daiqian"
	file_doc = ""
	file_xls = ""
	file_doc_pdf = ""
	file_xls_pdf = ""
	filenames = os.listdir(dir)
	filenames.sort(key=lambda fn: os.path.getmtime(dir + "\\" + fn) if not os.path.isdir(dir + "\\" + fn) else 0)
	#最新文件在最后，所以全扫完，最后一次命名为所需文件。
	for filename in filenames:
		if(filename.find(ent_nm)>=0 and filename.find("_定量分析.doc")>=0):
			file_doc = os.path.join(dir,filename)
			aes_encrypt(file_doc,bank_nm,"daiqian")
		if(filename.find(ent_nm)>=0 and filename.find("_定量评分.xls")>=0):
			file_xls = os.path.join(dir,filename)
			aes_encrypt(file_xls,bank_nm,"daiqian")
		if(filename.find(ent_nm)>=0 and filename.find("_定量分析.pdf")>=0):
			file_doc_pdf = os.path.join(dir,filename)
			aes_encrypt(file_doc_pdf,bank_nm,"daiqian")
		if(filename.find(ent_nm)>=0 and filename.find("_定量评分.pdf")>=0):
			file_xls_pdf = os.path.join(dir,filename)
			aes_encrypt(file_xls_pdf,bank_nm,"daiqian")

def daihou_report_encrypt(ent_nm,bank_nm):
	dir = r"D:\report\ori\daihou"
	file_txt = ""
	filenames = os.listdir(dir)
	filenames.sort(key=lambda fn: os.path.getmtime(dir + "\\" + fn) if not os.path.isdir(dir + "\\" + fn) else 0)
	#最新文件在最后，所以全扫完，最后一次命名为所需文件。
	for filename in filenames:
		if(filename.find(ent_nm)>=0 and filename.find("_贷后.txt")>=0):
			file_txt = os.path.join(dir,filename)
			aes_encrypt(file_txt,bank_nm,"daihou")

if __name__ == "__main__":
	#传入参数：推送类型/企业名称/银行拼音如guandu
	push_type = sys.argv[1]
	if(push_type=='1'):
		daiqian_report_encrypt(sys.argv[2],sys.argv[3]) 
	elif(push_type=='2'):
		daihou_report_encrypt(sys.argv[2],sys.argv[3]) 
	else:
		print("ERROR!")