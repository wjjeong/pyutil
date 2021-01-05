# -*- coding: utf-8 -*-

import pandas as pd
import sys
import pymysql
import datetime
import time
import re


def getPoListAll(cursor,schema):
    
    sql = """SELECT *
    FROM """+schema+""".ad_info_test  a
    """
    rows = ""

    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except Exception as e :
        print(e)
        now = datetime.datetime.now()
        print("["+now.strftime('%Y-%m-%d %H:%M:%S')+"] reconnect ")        
    
    return rows


def processExtrJpg(schema):
    now = datetime.datetime.now()    
    print("["+now.strftime('%Y-%m-%d %H:%M:%S')+"] Content replace Started :"+schema)


    # f = open(schema+".log", 'w')


    masterConnInfo = {'host': '192.168.0.153', 'port': 3306, 'user': 'report', 'passwd': 'jasonrp@#!', 'charset': 'euckr'}
    masterConn = pymysql.connect(**masterConnInfo)
    masterCurs = masterConn.cursor()


    rows = getPoListAll(masterCurs,schema)

    # print(len(rows))
    print(rows)
    # for row in rows:
    #     # print(schema+" : rows result")
    #     po_idx = row[0]
    #     po_content  = row[1].decode('utf-8')
    #     torder_po_idx = row[2]
    #     # print("원본")
    #     # print(po_content)
        
        
    #     blocks = re.findall('<.+?>', po_content)
    #     for block in blocks:
    #         image_list = re.findall('editor/(.+?jpg)', block)
    #         # print(po_idx)
    #         # print(image_list)
    #         for image in image_list :
    #             f.write(schema+","+str(po_idx)+","+str(torder_po_idx)+","+image+"\n")

    #     # break
    #     # upd_po_content = re.sub('http(.+?)simsale.kr', '', upd_po_content)
    #     # upd_po_content = re.sub('http(.+?)market09.kr', '', upd_po_content)
    #     # print("제거")
    #     # print(upd_po_content)
    #     # break
    #     # insertPoListMap(masterCurs,schema,po_idx,po_content,upd_po_content.encode('utf-8'))

    masterConn.commit()
    masterConn.close()
    # f.close()

    now = datetime.datetime.now()
    print("["+now.strftime('%Y-%m-%d %H:%M:%S')+"] Content replace completed")

# processExtrJpg("adunit")




print("")

conn = pymysql.connect(
    user='jason_da', 
    passwd='da@0417', 
    host='10.20.10.59', 
    db='jasonapp014', 
    charset='utf8'
)

cursor = conn.cursor()


sql = """#CREATE TABLE jasonapp014.da_pants_stat_temp AS			
SELECT	po.po_idx	
		,po.po_cate	
		,po.po_title	
#		,cate.po_cate_name	
#		,cate2.po_cate_name po_cate_name2	
#		,cate2.po_cate_idx	
#		,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(item.oValue,'CPLU30',''),'CPBN30',''),'CPPS300',''),'1030',''),'CPUWK3002',''),'M20',''),'WG15PT20',''),'G15PT720',''),'20S50',''),'20M50',''),'203-',''),'20M',''),'20SS',''),'209',''),'20A5',''),'201',''),'P20',''),'20A',''),'20H',''),'20P',''),'20W',''),'C20',''),'20B',''),'225',''),'224',''),'1022','') ,'922',''),'M22',''),'221',''),'127',''),'T27',''),'R27',''),'270',''),'128',''),'928',''),'528',''),'281',''),'029',''),'329',''),'291',''),'529',''),'265',''),'261',''),'526','') oValue	
		,item.oValue
		,f.wmsname	
FROM	jasonapp014.ord_order ord		
	INNER JOIN jasonapp014.ord_item item ON ord.ordnum = item.ordnum		
	INNER JOIN jasonapp014.po_list po ON item.po_idx = po.po_idx		
#	LEFT JOIN jasonapp018.da_po_cate cate ON cate.app = 'jasonapp014' AND cate.po_cate = po.po_cate		
	LEFT OUTER JOIN jasonapp014.po_wms f  ON po.po_wmsid = f.wmsid		
#	INNER JOIN jasonapp014.po_list_cate_20180912 cate2 ON po.po_cate = cate2.po_parent_idx AND po.po_cate2 = cate2.po_cate_idx		
	WHERE	orddate BETWEEN '2020-09-21 00:00:00' AND '2020-12-20 23:59:59'	
	AND		ord.ordflag = 'order'
	AND		ord.step1 IN ('R','S')
	AND 		ord.paydate IS NOT NULL
	AND		item.ordflag2 = 'order'
	AND		item.step2 IN ('1R','1S','1D','1C')
	AND		item.refund != 1
	AND 		po.po_cate = 40
	#AND 		po.po_cate2 = 65
#	AND po.po_title LIKE '%바지%'		
#GROUP BY po.po_idx, po.po_cate, item.oValue			
limit 10000
"""
cursor.execute(sql)
rows = cursor.fetchall()

# print(result)

# print(len(rows))
print(rows)

df = pd.DataFrame(index=range(0,1), columns=['size'])

for row in rows:
    # print(schema+" : rows result")
    # po_idx = row[0]
    # po_content  = row[1].decode('utf-8')
    # torder_po_idx = row[2]
    # print("원본")
    # print(po_content)
    po_idx = row[0]
    po_cate = row[1]
    po_title = row[2]
    ovalue = row[3]
    
    # chunks = ovalue.split()
    chunks = re.split('/|,|\)|\(| ', ovalue)
    # print(chunks)
    # blocks = re.findall('<.+?>', po_content)
    i = 0
    p = re.compile('[0-9]{1,2}')
    pSize = re.compile('[0-9\-XLMS~]{1,2}')
    pex1 = re.compile('^[ABCDEFGHIJKNOPQRTUVWYZ]\w+')
    for chunk in chunks:
        if (i == 0 and  p.match(chunk) == None) or i != 0:
            if chunk != '':
                if pSize.search(chunk) != None and len(chunk)<=5 and pex1.match(chunk) == None:
                    # print("size : "+chunk)
                    df = df.append({'size' : chunk } , ignore_index=True)
        
        i += 1


print(df)
grouped = df.groupby('size')
sizedf = pd.DataFrame(grouped.count()) # DataFrame 
print(grouped)
print(sizedf)
