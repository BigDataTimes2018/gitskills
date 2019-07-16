import pymysql
import time
import datetime
import os
from lxml import etree
import  re
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
	conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='yst@2019',db='rdc')
	cur = conn.cursor()
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
	cur.close()
	conn.close()


def value_added_tax_analysis(file_path):
    print(file_path)
    page = get_page(file_path)
    try:
        tax_date_section = page.xpath('//*[@id="MyDiv"]/table[1]//tr[5]/td[1]/text()')[0]
    except Exception as e:
        print("Error:" + str(e) + "\n")
        with open(r"D:\uploadTempFiles\python\logFile\error_else.txt", "a") as f_else:
            f_else.write(
                str(datetime.now()) + "\n" + file_path + "\n" + "program line 184. Table error. " + str(e) + "\n\n")
        return -1

    date_list = re.findall(r"\d+", tax_date_section)
    if (len(date_list) == 6):
        begin_date = date_list[0] + "-" + date_list[1] + "-" + date_list[2]
        end_date = date_list[3] + "-" + date_list[4] + "-" + date_list[5]
    else:
        print("Error when parse:" + file_path + "\n")
        with open(r"D:\uploadTempFiles\python\logFile\error_else.txt", "a") as f_else:
            f_else.write(
                str(datetime.now()) + "\n" + file_path + "\n" + "program line 194. Parse table period error.\n\n")
        return -1
    cycle_typ = report_cycle_typ(begin_date, end_date)

    ent_id = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td[2]/text()')[0]
    ent_nm = page.xpath('//*[@id="MyDiv"]/table[2]//tr[2]/td[2]/text()')[0]
    ent_indu = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td[4]/text()')[0]
    ent_type = page.xpath('//*[@id="MyDiv"]/table[2]//tr[3]/td[5]/text()')[0]

    list = page.xpath('//*[@id="MyDiv"]/table[3]//tr')

    detail_length = 0
    for detail in list:
        detail_length += 1
        if (detail_length < 3 or detail_length > 40):
            continue
        items = detail.xpath('./td')
        item_str = "insert ignore into  l1_value_added_tax values(sysdate(),'" + ent_id + "','" + begin_date + "','" + end_date + "','" + cycle_typ + "'"
        item_length = 0
        for item in items:
            item_length += 1
            if (len(items) > 6 and item_length == 1):
                continue
            content = item.xpath('./text()')[0]
            content = content.replace("\u3000", "").replace("\xa0", "")
            content = content.replace("\n", "")
            if (len(items) == 6 and item_length == 2 and len(content) > 2):
                content = content[:2]
            item_str = item_str + ",'" + content + "'"
        item_str = item_str + ");"
        mysql_execute(item_str, file_path)


def value_added_tax_analysis_small_scale(file_path):
    page = get_page(file_path)

    try:
        tax_date_section = page.xpath('//*[@id="MyDiv"]/table[2]//tr[3]/td[1]/text()')[0]
    except Exception as e:
        return -1

    date_list = re.findall(r"\d+", tax_date_section)
    if (len(date_list) == 6):
        begin_date = date_list[0] + "-" + date_list[1] + "-" + date_list[2]
        end_date = date_list[3] + "-" + date_list[4] + "-" + date_list[5]
    else:
        return -1
    cycle_typ = report_cycle_typ(begin_date, end_date)
    ent_id = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td/text()')[0]
    ent_id = ent_id.split(":")[1].replace(" ", "")
    ent_nm = page.xpath('//*[@id="MyDiv"]/table[2]//tr[2]/td/text()')[0]
    ent_nm = ent_nm.split("：")[1]
    list = page.xpath('//*[@id="MyDiv"]/table[3]//tr')
    detail_length = 0
    for detail in list:
        detail_length += 1
        if (detail_length < 3 or detail_length > 24):
            continue
        items = detail.xpath('./td')
        # item_str = "insert ignore into l1_value_added_tax_small_scale values(sysdate(),'" + ent_id + "','" + begin_date + "','" + end_date + "','" + cycle_typ + "'"
        item_length = 0
        for item in items:
            item_length += 1
            if (len(items) > 6 and item_length == 1):
                continue
            content = item.xpath('./text()')
            if (len(content) > 0):
                content = content[0]
                content = content.replace("\u3000", "").replace("\xa0", "")
                content = content.replace("\n", "")
                if (len(items) == 6 and item_length == 2 and len(content) > 2):
                    content = re.findall(r"\d+", content)[0]
                item_str = item_str + ",'" + content + "'"
            else:
                item_str = item_str + ",''"
        item_str = item_str + ");"
        # mysql_execute(item_str, file_path)


def purchase_tax_analysis(file_path, ent_id):
    page = get_page(file_path)

    try:
        tax_date_section = page.xpath('//*[@id="MyDiv"]/table[1]//tr[4]/td/div/text()')[0]
    except Exception as e:
        print("Error:" + str(e) + "\n")
        with open(r"D:\uploadTempFiles\python\logFile\error_else.txt", "a") as f_else:
            f_else.write(
                str(datetime.now()) + "\n" + file_path + "\n" + "program line 292. Table error. " + str(e) + "\n\n")
        return -1

    date_list = re.findall(r"\d+", tax_date_section)
    if (len(date_list) == 6):
        begin_date = date_list[0] + "-" + date_list[1] + "-" + date_list[2]
        end_date = date_list[3] + "-" + date_list[4] + "-" + date_list[5]
    else:
        print("Error when parse:" + file_path + "\n")
        with open(r"D:\uploadTempFiles\python\logFile\error_else.txt", "a") as f_else:
            f_else.write(
                str(datetime.now()) + "\n" + file_path + "\n" + "program line 302. Parse table period error.\n\n")
        return -1
    cycle_typ = report_cycle_typ(begin_date, end_date)

    # ent_id = page.xpath('//*[@id="MyDiv"]/table[2]//tr[1]/td[2]/text()')[0]
    ent_nm = page.xpath('//*[@id="MyDiv"]/table[1]//tr[6]/td[1]/text()')[0]
    ent_nm = ent_nm.replace(u"纳税人名称：", "")
    ent_nm = ent_nm.replace(u"（公章）", "")

    list = page.xpath('//*[@id="MyDiv"]/table[2]//tr')

    detail_length = 0
    for detail in list:
        detail_length += 1
        items = detail.xpath('./td')
        if (len(
                items) < 3 or detail_length == 2 or detail_length == 16 or detail_length == 29 or detail_length == 42):  # 标题行
            continue
        item_str = "insert ignore into l1_purchase_tax values(sysdate(),'" + ent_id + "','" + begin_date + "','" + end_date + "','" + cycle_typ + "'"
        item_length = 0
        for item in items:
            item_length += 1
            content = item.xpath('./text()')[0]
            content = content.replace("\u3000", "").replace("\xa0", "")
            content = content.replace("\n", "")
            content = content.replace("──", "")
            if (item_length == 2 and len(content) > 2):
                content = re.findall(r"\d+", content)[0]
            if (len(items) == 3 and item_length == 3):
                item_str = item_str + ",'',''"
            item_str = item_str + ",'" + content + "'"
        item_str = item_str + ");"
        if (item_str.find(",'8a',") > 0):
            detail_length -= 1
            continue
        item_str = item_str.replace("'8b'", "'8'")
        mysql_execute(item_str, file_path)


def purchase_detail_analysis(file_path):
    page = get_page(file_path)

    try:
        table_name = page.xpath('/html/body/div[1]/table[1]//tr[3]/td/text()')[0]
        if (table_name.find(u"申报抵扣明细") < 0):
            try:
                table_name = page.xpath('/html/body/div[1]/table[1]//tr[3]/td/p/text()')[0]
            except Exception as e:
                with open(r"D:\uploadTempFiles\python\logFile\error_else.txt", "a") as f_else:
                    f_else.write(str(datetime.now()) + "\n" + file_path + "\nTable error. " + str(e) + "\n\n")
                return -1
    except Exception as e:
        if (str(e) != "list index out of range"):
            with open(r"D:\uploadTempFiles\python\logFile\error_else.txt", "a") as f_else:
                f_else.write(str(datetime.now()) + "\n" + file_path + "\nTable error. " + str(e) + "\n\n")
        return -1
    if (table_name.find(u"申报抵扣明细") < 0):
        return -1
    try:
        ent_id = str(page.xpath('/html/body/div[1]/table[1]//tr[5]/td/text()')[0]).replace(" ", "")
        if (len(ent_id) < 10):
            ent_id = page.xpath('/html/body/div[1]/table[1]//tr[5]/td/p/text()')[0]
        deduct_period = page.xpath('/html/body/div[1]/table[1]//tr[4]/td/text()')[0]
    except Exception as e:
        print("Error:" + str(e) + "\n")
        with open(r"D:\uploadTempFiles\python\logFile\error_else.txt", "a") as f_else:
            f_else.write(
                str(datetime.now()) + "\n" + file_path + "\n" + "program line 364. Parse table header error. " + str(
                    e) + "\n\n")
        return -1

    pattern = re.compile(r'[A-Z0-9]+')
    ent_id = pattern.findall(ent_id)[0]
    deduct_period = pattern.findall(deduct_period)
    deduct_period = "-".join(deduct_period)

    tables = page.xpath('//*[@class="pageEnd"]')
    # table_version = 0 # 记录进项明细表格式，不同格式不同结构
    for table in tables:
        list = table.xpath('./table[2]//tr')
        detail_length = 0
        for detail in list:
            detail_length += 1
            if (detail_length == 1):
                continue
            items = detail.xpath('./td')

            i = 1 if len(items) == 10 else 0
            detail_no = items[0 + i].xpath('./text()')[0].replace("\u3000", "").replace("-", "")
            receipt_no = items[1 + i].xpath('./text()')[0].replace("\u3000", "")
            if (len(receipt_no) > 0 and detail_no.find("计") == -1):
                receipt_id = items[2 + i].xpath('./text()')[0].replace("\u3000", "")
                receipt_date = items[3 + i].xpath('./text()')[0].replace("\u3000", "")
                amount = items[4 + i].xpath('./text()')[0].replace("\u3000", "")
                tax_amt = items[5 + i].xpath('./text()')[0].replace("\u3000", "")
                sale_ent_id = items[6 + i].xpath('./text()')[0].replace("\u3000", "")
                deduct_date = items[7 + i].xpath('./text()')[0].replace("\u3000", "")
                remarks = items[8 + i].xpath('./text()')[0].replace("\u3000", "")
                item_str = "insert ignore into l1_purchase_detail values(sysdate(),'" + ent_id + "','" + \
                           deduct_period + "','" + detail_no + "','" + receipt_no + "','" + receipt_id + "','" + receipt_date + "','" + amount + "','" + \
                           tax_amt + "','" + sale_ent_id + "','" + deduct_date + "','" + remarks + "');"
                mysql_execute(item_str, file_path)

        if (len(table.xpath('./table[2]/td')) > 9 and len(table.xpath('./table[2]/td')) % 9 == 0):
            td_list = table.xpath('./table[2]/td')
            for i in range(0, len(td_list), 9):
                detail_no = td_list[i].xpath('./text()')[0].replace("\u3000", "").replace("-", "")
                receipt_no = td_list[i + 1].xpath('./text()')[0].replace("\u3000", "")
                if (len(receipt_no) > 0 and detail_no.find("计") == -1):
                    receipt_id = td_list[i + 2].xpath('./text()')[0].replace("\u3000", "")
                    receipt_date = td_list[i + 3].xpath('./text()')[0].replace("\u3000", "")
                    amount = td_list[i + 4].xpath('./text()')[0].replace("\u3000", "")
                    tax_amt = td_list[i + 5].xpath('./text()')[0].replace("\u3000", "")
                    sale_ent_id = td_list[i + 6].xpath('./text()')[0].replace("\u3000", "")
                    deduct_date = td_list[i + 7].xpath('./text()')[0].replace("\u3000", "")
                    remarks = td_list[i + 8].xpath('./text()')[0].replace("\u3000", "")
                    item_str = "insert ignore into l1_purchase_detail values(sysdate(),'" + ent_id + "','" + \
                               deduct_period + "','" + detail_no + "','" + receipt_no + "','" + receipt_id + "','" + receipt_date + "','" + amount + "','" + \
                               tax_amt + "','" + sale_ent_id + "','" + deduct_date + "','" + remarks + "');"
                    mysql_execute(item_str, file_path)
                else:
                    break

def all_analysis(file_path):
    report_typ = 0
    if (file_path.find(u"2013版财务报表") >= 0):
        report_typ = 1
    elif (file_path.find(u"增值税.") >= 0):
        report_typ = 2
    else:
        return -1
    print(file_path)
    page = get_page(file_path)
    try:
        ent_id = page.xpath('/html//table[1]//tr[3]/td[1]/b/text()')[0]
        ent_id = ent_id.split("：")[1]
        list = page.xpath('/html//table[2]//tr')
    except Exception as e:
        return -1

    detail_length = 0
    for detail in list:
        detail_length += 1
        if (detail_length == 1):
            continue
        items = detail.xpath('./td')
        print(len(items))
        if (len(items) < 7):
            continue
        item_content = []
        item_content.append(ent_id)
        item_length = 0
        for item in items:
            item_length += 1
            if (item_length == 7):
                continue
            content = item.xpath('./div/text()')[0]
            content = content.replace("\u3000", "")
            item_content.append(content)
            if (item_length == 5):
                cycle_typ = report_cycle_typ(item_content[-2], item_content[-1])
                item_content.append(cycle_typ)
        item_str = "insert ignore into l1_all_list values(sysdate(),'" + "','".join(item_content) + "');"
        if (len(item_content) == 8):
            pass
            # mysql_execute(item_str, file_path)

        if (item_content[2].find(u"小规模纳税人适用") >= 0):
            report_typ = 3
        elif (item_content[2].find(u"一般纳税人适用") >= 0):
            report_typ = 2
        elif (item_content[2].find(u"增值税预缴税款表") >= 0):
            report_typ = 4
        if (report_typ == 1):  # 财务报表，包含资产负债表和利润表
            next_file_path = file_path.split(".")[0] + "." + item_content[1]
        elif (report_typ == 2):  # 一般纳税人的增值税申报表
            next_file_path = file_path.split(".")[0] + "." + item_content[1]
            if (os.path.exists(next_file_path + ".1.html")):
                value_added_tax_analysis(next_file_path + ".1.html")  # 增值税总表
            if (os.path.exists(next_file_path + ".3.html")):
                purchase_tax_analysis(next_file_path + ".3.html", ent_id)  # 进项税额统计表
            if (os.path.exists(next_file_path + ".4.html")):
                pass
                purchase_detail_analysis(next_file_path + ".4.html")  # 进项抵扣明细表
        elif (report_typ == 3):  # 小规模纳税人适用增值税申报表
            next_file_path = file_path.split(".")[0] + "." + item_content[1]
            print(next_file_path)
            if (os.path.exists(next_file_path + ".1.html")):
                print("1.小微企业进行了调用")
                value_added_tax_analysis_small_scale(next_file_path + ".1.html")
        elif (report_typ == 4):  # 增值税预缴税
            # 款表
            print(u"增值税预缴税款表")

if __name__ == '__main__':
    # all_analysis("C:/Users/pc/Desktop/91530121MA6K73TL9D_shenbao")
    all_analysis("C:/Users/pc/Desktop/91530121MA6K73TL9D_shenbao/20160712_20190712_2013版财务报表.all.html")
