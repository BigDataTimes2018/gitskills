# -*- coding: utf-8 -*-
import comtypes.client
import xlwt
import xlrd
from xlutils.copy import copy
import pandas as pd
import numpy as np
import pymysql
from docx import Document
import logging
import pandas.io.sql as sql
from datetime import datetime,date
import os
import uuid
import hashlib
from win32com.client import Dispatch, constants, gencache, DispatchEx
import re
import pandas as pd
#-*- coding : utf-8 -*-
# coding: utf-8
# 方法一：
# def docToPDF(file_path):
#     pdf_path = file_path.replace(".doc",".pdf")
#     # taskkill 终止指定进程程序  /f表示强制执行
#     stat = os.system('taskkill /im wps.exe')
#     os.system("start wps.exe")
#     # 调用wps v9进行
#     o = Dispatch("wps.Application")
#     # 是否显示页面
#     o.Visible=False
#     doc = o.Documents.Open(file_path)
#     doc.ExportAsFixedFormat(pdf_path,17)
#     o.Quit()
# if __name__ == '__main__':
#     docToPDF("D:\pycharm_data\doc_input.docx")

# 方法二
# def convertDocxToPDF(infile):
#     outfile = infile.replace(".doc", ".pdf")
#     word = comtypes.client.CreateObject('Word.Application')
#     doc = word.Documents.Open(infile)
#     doc.SaveAs(outfile, 17)
#     doc.Close()
#     word.Quit()
# if __name__ == '__main__':
#     convertDocxToPDF("D:/uploadTempFiles/python/云南甲子科技有限公司_20190628_定量分析.doc")

# 方法三：
# def docToPdf(input):
#     output=input.replace(".doc",".pdf")
#     w = Dispatch("Word.Application")
#     doc = w.Documents.Open(input, ReadOnly=1)
#     doc.ExportAsFixedFormat(output, 17)
#     doc.Close()
#     w.Quit()
# docToPdf("D:\pycharm_data\doc_input.doc")



# def doc2pdf(doc_name, pdf_name):
#     """
#     :word文件转pdf
#     :param doc_name word文件名称
#     :param pdf_name 转换后pdf文件名称
#     """
#     try:
#         word = DispatchEx("Word.Application")
#         if os.path.exists(pdf_name):
#             os.remove(pdf_name)
#         worddoc = word.Documents.Open(doc_name,ReadOnly = 1)
#         worddoc.SaveAs(pdf_name, FileFormat = 17)
#         worddoc.Close()
#         return pdf_name
#     except:
#         return 1
# if __name__=='__main__':
#     doc_name = "D:\pycharm_data\doc_input.doc"
#     ftp_name = "D:\pycharm_data\doc_input.pdf"
#     doc2pdf(doc_name, ftp_name)


# -*- coding: utf-8 -*-
# import os
# from win32com import client
# def doc2pdf(doc_name, pdf_name):
#     try:
#         word = DispatchEx("Word.Application")
#         if os.path.exists(pdf_name):
#             os.remove(pdf_name)
#         worddoc = word.Documents.Open(doc_name,ReadOnly = 1)
#         worddoc.SaveAs(pdf_name, FileFormat = 17)
#         worddoc.Close()
#         return pdf_name
#     except:
#         return 1
# if __name__=='__main__':
#     doc_name = "D:\pycharm_data\doc_input.doc"
#     ftp_name = "D:\pycharm_data\doc_input.pdf"
#     doc2pdf(doc_name, ftp_name)



# m = hashlib.md5()
# print(m)
# str="语言"
# b = str.encode(encoding='utf-8')
# key=m.update(b)
# key = m.hexdigest()
# key = key[:16]
# print(key)

# data = "你好"
# m = hashlib.md5(data.encode("gb2312"))
# print(m.hexdigest())
#
# data = "你好"
# m = hashlib.md5(data.encode("utf-8"))
# print(m.hexdigest())
# def find_dir(file_path):
#     dirs=os.listdir(file_path)
#     for dir in dirs:
#         if "91530100MA6K3YXF8W" in dir:
#             print(dir)
# if __name__ == '__main__':
#     find_dir("D:/uploadTempFiles/shenbao/DATA/20190506/91")

def test_empty(filePath):
    # dtype=str 表示读取的时候按照字符串的方式进行读取
    begin_time=datetime.now()
    data=pd.read_csv(filePath,sep=",",header=None,encoding="utf8",dtype=str)
    data = data.dropna(axis=0, how='all')
    # quoting=1 表示存储的时候加上双引号
    data.to_csv(filePath,quoting=1,sep=',',header=False,index=False)
    end_time=datetime.now()
    print("程序总共运行的时间是: "+str(end_time-begin_time))
if __name__ == '__main__':
    test_empty("C:/Users/pc/Desktop/91530121MA6NEG8W1J_output/zzs_fpkj_mxjxx.txt")

# def test_empy(filePath):
#     number=0
#     begin_time = datetime.now()
#     with open(filePath,"r+") as f:
#         line=f.readline()
#         words=line.split(",")
#         for word in words:
#             if(word=='""'):
#                 number=number+1
#         print(number)
#         # 说明首行是空值
#         if(number==67):
#             texts=f.readlines()
#             f.seek(0)
#             f.truncate()
#             for text in texts:
#                 f.write(str(text))
#     end_time = datetime.now()
#     print("程序总共运行的时间是: " + str(end_time - begin_time))
# if __name__ == '__main__':
#     test_empy("C:/Users/pc/Desktop/aaa/91530121MA6NEG8W1J_output/zzs_fpkj.txt")



