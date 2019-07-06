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

def aes_encrypt(ent_nm,bank_nm):
	
	dir = r"D:\report\ori\daiqian"
	file_doc = ""
	file_xls = ""
	file_doc_pdf = ""
	file_xls_pdf = ""
	filenames = os.listdir(dir)
    # 按照文件最后的修改时间进行排序
	filenames.sort(key=lambda fn: os.path.getmtime(dir + "\\" + fn) if not os.path.isdir(dir + "\\" + fn) else 0)
	#最新文件在最后，所以全扫完，最后一次命名为所需文件。
	for filename in filenames:
		if(filename.find(ent_nm)>=0 and filename.find("_定量分析.doc")>=0):
			file_doc = os.path.join(dir,filename)
		if(filename.find(ent_nm)>=0 and filename.find("_定量评分.xls")>=0):
			file_xls = os.path.join(dir,filename)
		# if(filename.find(ent_nm)>=0 and filename.find("_定量分析.pdf")>=0):
			# file_doc_pdf = os.path.join(dir,filename)
		# if(filename.find(ent_nm)>=0 and filename.find("_定量评分.pdf")>=0):
			# file_xls_pdf = os.path.join(dir,filename)

	# md5加密
	m = hashlib.md5()
	str = file_doc.split("\\")[-1].split("_")[0]+bank_nm+"mP3s#&s" # 加密key为企业名称+银行名+随机码
	# 再进行md5哈希运算之前，需要对数据进行编码
	b = str.encode(encoding='utf-8')
	m.update(b)
	# 加密之后用16进制进行保密
	key = m.hexdigest()
	# key = key + "Q823#MS9$~01"
	key = key.encode(encoding="utf-8")
	key = key[:16]

	# 文件内容用md5加密，并写进检核文件。
	f_doc =open(file_doc,mode='rb')
	text_doc = f_doc.read()
	m.update(text_doc)
	md5_text_doc = m.hexdigest() #写检核文件用
	f_doc.close()

	f_xls =open(file_xls,mode='rb')
	text_xls = f_xls.read()
	m.update(text_xls)
	md5_text_xls = m.hexdigest() #写检核文件用
	f_xls.close()

	# f_doc_pdf =open(file_doc_pdf,mode='rb')
	# text_doc_pdf = f_doc_pdf.read()
	# m.update(text_doc_pdf)
	# md5_text_doc_pdf = m.hexdigest() #写检核文件用
	# f_doc_pdf.close()

	# f_xls_pdf =open(file_xls_pdf,mode='rb')
	# text_xls_pdf = f_xls_pdf.read()
	# m.update(text_xls_pdf)
	# md5_text_xls_pdf = m.hexdigest() #写检核文件用
	# f_xls_pdf.close()

	aes = AES.new(key, AES.MODE_ECB) # 初始化aes加密器

	# 文件内容用aes加密处理
	encrypted_doc = base64.b64encode(aes.encrypt(bytePad(text_doc))).decode().replace('\n', '')
	encrypted_xls = base64.b64encode(aes.encrypt(bytePad(text_xls))).decode().replace('\n', '')
	# encrypted_doc_pdf = base64.b64encode(aes.encrypt(bytePad(text_doc_pdf))).decode().replace('\n', '')
	# encrypted_xls_pdf = base64.b64encode(aes.encrypt(bytePad(text_xls_pdf))).decode().replace('\n', '')
	# print(encrypted_text)

	# 新建日期文件夹
	datedirpath = "D:/report/"+bank_nm+"/daiqian/"+date.today().strftime("%Y-%m-%d")
	if not os.path.exists(datedirpath):
		os.makedirs(datedirpath)
		f_blank = open(datedirpath+"/"+date.today().strftime("%Y-%m-%d")+"_OK.txt","w")
		f_blank.close()

	# 新建企业名文件夹
	dirpath = "D:/report/"+bank_nm+"/daiqian/"+date.today().strftime("%Y-%m-%d")+"/"+ent_nm
	if not os.path.exists(dirpath):
		os.makedirs(dirpath)

	# aes加密后内容写入加密文件
	f_encrypt_doc = open(dirpath+"/"+file_doc.split("\\")[-1].replace(".doc",".doc.enc"),'w')
	f_encrypt_doc.write(encrypted_doc)
	f_encrypt_doc.close()

	f_encrypt_xls = open(dirpath+"/"+file_xls.split("\\")[-1].replace(".xls",".xls.enc"),'w')
	f_encrypt_xls.write(encrypted_xls)
	f_encrypt_xls.close()

	# f_encrypt_doc_pdf = open(dirpath+"/"+file_doc_pdf.split("\\")[-1].replace(".pdf",".pdf.enc"),'w')
	# f_encrypt_doc_pdf.write(encrypted_doc_pdf)
	# f_encrypt_doc_pdf.close()

	# f_encrypt_xls_pdf = open(dirpath+"/"+file_xls_pdf.split("\\")[-1].replace(".pdf",".pdf.enc"),'w')
	# f_encrypt_xls_pdf.write(encrypted_xls_pdf)
	# f_encrypt_xls_pdf.close()

	# 写检核文件
	f_check = open(dirpath+"/"+file_doc.split("\\")[-1].split("_")[0]+"_检核.ok",'w')

	f_encrypt_doc = open(dirpath+"/"+file_doc.split("\\")[-1].replace(".doc",".doc.enc"),'rb')
	f_encrypt_doc_text = f_encrypt_doc.read()
	m.update(f_encrypt_doc_text)
	f_check.write(file_doc.split("\\")[-1].replace(".doc",".doc.enc")+'\t'+md5_text_doc+'\t'+m.hexdigest()+'\n')
	f_encrypt_doc.close()

	f_encrypt_xls = open(dirpath+"/"+file_xls.split("\\")[-1].replace(".xls",".xls.enc"),'rb')
	f_encrypt_xls_text = f_encrypt_xls.read()
	m.update(f_encrypt_xls_text)
	f_check.write(file_xls.split("\\")[-1].replace(".xls",".xls.enc")+'\t'+md5_text_xls+'\t'+m.hexdigest()+'\n')
	f_encrypt_xls.close()

	# f_encrypt_doc_pdf = open(dirpath+"/"+file_doc_pdf.split("\\")[-1].replace(".pdf",".pdf.enc"),'rb')
	# f_encrypt_doc_pdf_text = f_encrypt_doc_pdf.read()
	# m.update(f_encrypt_doc_pdf_text)
	# f_check.write(file_doc_pdf.split("\\")[-1].replace(".pdf",".pdf.enc")+'\t'+md5_text_doc_pdf+'\t'+m.hexdigest()+'\n')
	# f_encrypt_doc_pdf.close()

	# f_encrypt_xls_pdf = open(dirpath+"/"+file_xls_pdf.split("\\")[-1].replace(".pdf",".pdf.enc"),'rb')
	# f_encrypt_xls_pdf_text = f_encrypt_xls_pdf.read()
	# m.update(f_encrypt_xls_pdf_text)
	# f_check.write(file_xls_pdf.split("\\")[-1].replace(".pdf",".pdf.enc")+'\t'+md5_text_xls_pdf+'\t'+m.hexdigest()+'\n')
	# f_encrypt_xls_pdf.close()

	f_check.close()


if __name__ == "__main__":
	#传入参数：企业名称/银行拼音如guandu
	aes_encrypt(sys.argv[1],sys.argv[2]) 