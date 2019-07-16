import pymysql
import traceback
import logging
import time
from lxml import etree
import re
import sys
import os
from warnings import filterwarnings
from datetime import datetime,date,timedelta
from aes_encrypt2 import daihou_report_encrypt

filterwarnings('error', category = pymysql.Warning)

# 本程序为贷后风险监控报告的输出脚本
# 从loan_invest_info中抽取需要进行贷后监控的企业，按需输出其预警内容。
#筛选一下，有风险，才推送。且只推送风险部分。

conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='yst@2019',db='rdc')
cur = conn.cursor()

def output_risk_report(ent_tax_id):
	
	resStr = ""
	resList = []
	outputFlag = 0 #如若有风险，置为1，并输出报告。
	
	sqlStr = "select ent_nm from rdc.dim_ent_info where ent_tax_id = '"+ent_tax_id+"';"
	cur.execute(sqlStr)
	ent_nm = str(cur.fetchall()[0][0])
	
	resList.append(ent_tax_id)
	resList.append(ent_nm)
	
	resStr = ent_nm+" 预警信息（"+str(date.today().strftime("%Y-%m-%d"))+"）\n\n"
	print(resStr)
	# datediff()将日期相减得到天数
	sqlStr1 = "select max(receipt_date),datediff(curdate(),max(receipt_date)) from rdc.l2_receipt_detail where ent_tax_id = '"+ent_tax_id+"';"
	cur.execute(sqlStr1)
	res1 = cur.fetchall()[0]
	resStr1 = "增值税开票情况：\n\t借款人最近一次有效开票日期："+str(res1[0])\
	+"，距今"+str(res1[1])+"天。\n"
	resList.append(str(res1[0]))
	
	riskStr1 = "无。"
	if(int(res1[1])>60):
		riskStr1 = "Ⅰ级风险！"
	if(int(res1[1])>90):
		riskStr1 = "Ⅱ级风险！"
	if(int(res1[1])>120):
		riskStr1 = "Ⅲ级风险！"
	
	if(int(res1[1])>60): #有风险时才输出风险内容。
		outputFlag = 1
		resStr = resStr+resStr1+"\n\t风险："+riskStr1+"\n\n"
	print("1."+resStr1+"\n\t风险："+riskStr1+"\n")
	# date_sub 日期减去指定的时间间隔
	sqlStr2 = "select coalesce(amt1,0) amt1,coalesce(amt2,0) amt2,coalesce(amt1,0)+coalesce(amt2,0) amt_sum, \
	coalesce(amt1,0)-coalesce(amt2,0) amt_sub \
	from (select sum(case when receipt_flg IN ('0', '1') then amount else 0 end) as amt1 \
	from rdc.l2_receipt_detail \
	where ent_tax_id = '"+ent_tax_id+"' and receipt_date>=date_sub(curdate(),INTERVAL 30 day)) t1 \
	join \
	(select sum(case when receipt_flg IN ('0', '1') then amount else 0 end) as amt2 \
	from rdc.l2_receipt_detail \
	where receipt_date>=date_sub(curdate(),INTERVAL 60 day) and receipt_date<date_sub(curdate(),INTERVAL 30 day) \
	and ent_tax_id = '"+ent_tax_id+"') t2 \
	on 1=1;"
	cur.execute(sqlStr2)
	res2 = cur.fetchall()[0]
	resStr2 = ("增长"+str(res2[3])+"元。") if res2[3]>0 else ("下降"+str(res2[3])[1:]+"元。")
	resStr2 = "销售收入：\n\t最近60天该企业销售收入为"+str(res2[2])\
	+"元，其中前30天为"+str(res2[1])+"元，后30天为"+str(res2[0])+"元。\n\t后期比前期" +resStr2+"\n\n"
	resList.append(str(res2[2]))
	resList.append(str(res2[1]))
	resList.append(str(res2[0]))
	
	sqlStr3 = "select amt1,amt2,amt1-amt2 amt_sub, \
	round(100*(amt1-amt2)/amt2,2) as amt_sub_ratio \
	from \
	(select sum(case when receipt_flg IN ('0', '1') then amount else 0 end) as amt1 \
	from rdc.l2_receipt_detail \
	where ent_tax_id = '"+ent_tax_id+"' and receipt_date>=date_sub(curdate(),INTERVAL 90 day)) t1 \
	join \
	(select sum(case when receipt_flg IN ('0', '1') then amount else 0 end) as amt2 \
	from rdc.l2_receipt_detail \
	where receipt_date>=date_sub(date_sub(curdate(),INTERVAL 90 day),INTERVAL 1 YEAR) and receipt_date<date_sub(curdate(),INTERVAL 1 year) \
	and ent_tax_id = '"+ent_tax_id+"') t2 \
	on 1=1;"
	cur.execute(sqlStr3)
	res3 = cur.fetchall()[0]
	resStr3 = ("增长"+str(res3[2])+"元，增幅"+str(res3[3])+"%。") if float(res3[2])>0 else ("下降"+str(res3[2])[1:]+"元，降幅"+str(res3[3])[1:]+"%。")
	resStr3 = "\t最近90天该企业销售收入为"+str(res3[0])+"元，上年同期销售收入为"+str(res3[1])+"元。\n\t同比"+resStr3+"\n"
	resList.append(str(res3[0]))
	resList.append(str(res3[1]))
	
	riskStr3 = "无。"
	if(float(res3[3])<-20):
		riskStr3 = "Ⅰ级风险！"
	if(float(res3[3])<-30):
		riskStr3 = "Ⅱ级风险！"
	if(float(res3[3])<-50):
		riskStr3 = "Ⅲ级风险！"
	
	if(float(res3[3])<-20):
		outputFlag = 1
		resStr = resStr+resStr2+resStr3+"\n\t风险："+riskStr3+"\n\n"
	print("2."+resStr2+resStr3+"\n\t风险："+riskStr3+"\n")
	
	sqlStr4 = "select pro_nm from rdc.dim_ent_info where ent_tax_id = '"+ent_tax_id+"';"
	cur.execute(sqlStr4)
	res4 = cur.fetchall()[0]
	resStr4 = "主营商品：\n\t近12个月销售额最大的商品是："+res4[0]+"。\n"
	
	sqlStr5 = "select coalesce(amt1,0),coalesce(amt2,0),coalesce(amt1,0)-coalesce(amt2,0) amt_sub, \
	case when amt2 is null then 'no ratio' else round(100*(coalesce(amt1,0)-amt2)/amt2,2) end as amt_sub_ratio \
	from \
	(select sum(case when receipt_flg IN ('0', '1') then amount else 0 end) as amt1 \
	from (select ent_tax_id, receipt_flg, amount, tax_code \
	from rdc.l2_receipt_detail \
	where ent_tax_id = '"+ent_tax_id+"' and receipt_date>=date_sub(curdate(),INTERVAL 90 day)) t1 \
	join rdc.dim_ent_info ent \
	on t1.ent_tax_id = ent.ent_tax_id and left(t1.tax_code,7) = left(ent.pro_tax_cd,7)) t2 \
	join \
	(select sum(case when receipt_flg IN ('0', '1') then amount else 0 end) as amt2 \
	from (select ent_tax_id, receipt_flg, amount, tax_code \
	from rdc.l2_receipt_detail \
	where ent_tax_id = '"+ent_tax_id+"' \
	and receipt_date>=date_sub(date_sub(curdate(),INTERVAL 90 day),INTERVAL 1 YEAR) and receipt_date<date_sub(curdate(),INTERVAL 1 year)) t3 \
	join rdc.dim_ent_info ent \
	on t3.ent_tax_id = ent.ent_tax_id and left(t3.tax_code,7) = left(ent.pro_tax_cd,7)) t4;"
	cur.execute(sqlStr5)
	res5 = cur.fetchall()[0]
	resStr5 = ("增长"+str(res5[2])+"元，增幅"+str(res5[3])+"%。") if res5[2]>0 else ("下降"+str(res5[2])[1:]+"元，降幅"+str(res5[3])[1:]+"%。")
	resStr5 = "\n\t最近90天该企业该类商品销售收入为"+str(res5[0])+"元，上年同期销售收入为"+str(res5[1])+"元。\n\t同比"+resStr5+"\n"
	resList.append(str(res5[0]))
	resList.append(str(res5[1]))
	
	riskStr5 = "无。"
	if(float(res5[3])<-20):
		riskStr5 = "Ⅰ级风险！"
	if(float(res5[3])<-30):
		riskStr5 = "Ⅱ级风险！"
	if(float(res5[3])<-50):
		riskStr5 = "Ⅲ级风险！"
	
	if(float(res5[3])<-20):
		outputFlag = 1
		resStr = resStr+resStr4+resStr5+"\n\t风险："+riskStr5+"\n\n"
	print("3."+resStr4+resStr5+"\n\t风险："+riskStr5+"\n")
	
	sqlStr6 = "SELECT round(indu.indu_ent_cnt * (SUM(CASE WHEN indu.ind_value > target.ind_value THEN 1 ELSE 0 END)+1)/COUNT(*), 0) AS rank1 \
	, round(100*(SUM(CASE WHEN indu.ind_value > target.ind_value THEN 1 ELSE 0 END)+1)/COUNT(*), 2) AS pct \
	from ( SELECT fp.ent_tax_id, SUM(CASE WHEN receipt_flg IN ('0', '1') THEN amount ELSE 0 END) AS ind_value \
	, min(indu.indu_ent_cnt) as indu_ent_cnt \
	FROM l2_receipt_detail fp \
	JOIN dim_ent_info ent ON fp.ent_tax_id = ent.ent_tax_id \
	JOIN ( SELECT indu_cd, indu_ent_cnt FROM dim_ent_info WHERE ent_tax_id = '"+ent_tax_id+"') indu \
	ON ent.indu_cd = indu.indu_cd \
	WHERE receipt_date >= date_sub(curdate(), INTERVAL 90 day) AND receipt_date < curdate() \
	GROUP BY ent_tax_id ORDER BY ind_value DESC ) indu \
	JOIN ( SELECT SUM(CASE WHEN receipt_flg IN ('0', '1') THEN amount ELSE 0 END) AS ind_value \
	FROM l2_receipt_detail fp \
	WHERE (receipt_date >= date_sub(curdate(), INTERVAL 90 day) \
	AND receipt_date < curdate() \
	AND ent_tax_id = '"+ent_tax_id+"') \
	) target ON 1 = 1"
	cur.execute(sqlStr6)
	res6 = cur.fetchall()[0]
	resStr6 = "销售额本地行业排名：\n\t最近90天，销售额本地行业排名第"+str(res6[0])+"位，处于"+str(res6[1])+"%。\n"
	resList.append(str(res6[0]))
	
	sqlStr7 = "SELECT round(indu.indu_ent_cnt * (SUM(CASE WHEN indu.ind_value > target.ind_value THEN 1 ELSE 0 END) + 1) / COUNT(*), 0) AS rank1 \
	,round(100*(SUM(CASE WHEN indu.ind_value > target.ind_value THEN 1 ELSE 0 END)+1)/COUNT(*), 2) AS pct \
	from (SELECT fp.ent_tax_id, SUM(CASE WHEN receipt_flg IN ('0', '1') THEN amount ELSE 0 END) AS ind_value \
	, min(indu.indu_ent_cnt) as indu_ent_cnt \
	FROM l2_receipt_detail fp \
	JOIN dim_ent_info ent ON fp.ent_tax_id = ent.ent_tax_id \
	JOIN ( SELECT indu_cd, indu_ent_cnt FROM dim_ent_info WHERE ent_tax_id = '"+ent_tax_id+"' ) indu \
	ON ent.indu_cd = indu.indu_cd \
	WHERE receipt_date >= date_sub(curdate(), INTERVAL 180 day) \
	AND receipt_date < date_sub(curdate(), INTERVAL 90 day) \
	GROUP BY ent_tax_id ORDER BY ind_value DESC ) indu \
	JOIN ( SELECT SUM(CASE WHEN receipt_flg IN ('0', '1') THEN amount ELSE 0 END) AS ind_value \
	FROM l2_receipt_detail fp \
	WHERE (receipt_date >= date_sub(curdate(), INTERVAL 180 day) \
	AND receipt_date < date_sub(curdate(), INTERVAL 90 day) \
	AND ent_tax_id = '"+ent_tax_id+"') \
	) target ON 1 = 1"
	cur.execute(sqlStr7)
	res7 = cur.fetchall()[0]
	resStr7 = "\t上一个90天，销售额本地行业排名第"+str(res7[0])+"。位，处于"+str(res7[1])+"%。\n"
	resList.append(str(res7[0]))
	riskStr7 = "无。"
	descRes7 = round(100*(res6[0] - res7[0])/res7[0],2)
	if(descRes7>20):
		riskStr7 = "Ⅰ级风险！"
	if(descRes7>30):
		riskStr7 = "Ⅱ级风险！"
	if(descRes7>50):
		riskStr7 = "Ⅲ级风险！"
	
	if(descRes7>20):
		outputFlag = 1
		resStr = resStr+resStr6+resStr7+"\n\t风险："+riskStr7+"\n\n"
	print("4."+resStr6+resStr7+"\n\t风险："+riskStr7+"\n")
	
	if(outputFlag == 1):
		f = open(os.path.join(r"D:\report\ori\daihou",str(ent_nm)+"_"+str(date.today().strftime("%Y-%m-%d"))+"_贷后.txt"),'w')
		f.write(resStr)
		f.close()
	
	print(resList)
	
	return outputFlag

def risk_report_main():
	s = "select loan.ent_tax_id, reg.ent_name, clct.last_collection_date \
	from yst.loan_invest_info ivst \
	left join yst.loan_info loan on ivst.apply_id = loan.seria_id \
	left join yst.register_auth_info reg on loan.ent_tax_id = reg.ent_tax_id \
	left join (select ent_tax_id, max(push_time) push_time from yst.report_push_record group by ent_tax_id) push on loan.ent_tax_id = push.ent_tax_id \
	left join yst.tax_collection_record clct on loan.ent_tax_id = clct.taxcode \
	where last_collection_date>push_time and last_collection_date>date_sub(curdate(),interval 15 day) \
	order by clct.last_collection_date desc;"
	cur.execute(s)
	entList = cur.fetchall()
	for ent_info in entList:
		ent_tax_id = ent_info[0]
		ent_nm = ent_info[1]
		last_collection_date = ent_info[2]
		print(ent_nm+" 最后一次获取数据时间："+str(last_collection_date)+"\n")
		outputFlag = output_risk_report(ent_tax_id)
		if outputFlag:
			daihou_report_encrypt(ent_nm,"guandu")

if __name__=="__main__":
	# ent_tax_id = sys.argv[1]
	# output_risk_report(ent_tax_id)
	risk_report_main()
