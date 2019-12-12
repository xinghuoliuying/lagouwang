#!/usr/bin/env python
# coding: utf-8
# # 爬虫实战：拉勾网

from selenium import webdriver
from bs4 import BeautifulSoup
import os
import time
import pandas as pd
import numpy as np
import csv
import pymysql

firefox_driver = '/mnt/c/Users/Administrator/Desktop/python/Driver/geckodriver.exe'
driver = webdriver.Firefox(executable_path = firefox_driver)

db = pymysql.connect(host='127.0.0.1',user='root',password='123',database='scraping',charset='utf8') #连接MySQL数据库
cur = db.cursor()

job=[]       #建立空列表放入要抓取的信息
company=[]
salary=[]
experience=[]
education=[]
address=[]
describe=[]

def scraping(url):           #爬取页面信息
    driver.get(url)
    pageSource = driver.page_source
    content = BeautifulSoup(pageSource)
    
    for item in content.findAll('a',{"class":"position_link"}): #公司链接
        if "href" in item.attrs:
            link=item.attrs['href']
        driver.get(link)
        pageSource = driver.page_source
        content = BeautifulSoup(pageSource)
        time.sleep(5)
        
        data1 = content.findAll('span',{"class":"ceil-job"})[0].get_text().replace(' /','') #职位
        job.append(data1)
        
        data2 = content.findAll('img',{"class":"b2"})[0].attrs['alt']     #公司
        company.append(data2)
    
        data3 = content.findAll('span',{"class":"ceil-salary"})[0].get_text()     #工资
        salary.append(data3)
        
        data4 = content.h3.findAll('span',{"class":None})[1].get_text().replace(' /','') #经验
        experience.append(data4)
        
        data5 = content.h3.findAll('span',{"class":None})[2].get_text().replace(' /','') #学历
        education.append(data5)
        
        data6 = content.h3.findAll('span',{"class":None})[0].get_text().replace('/','')  #地点
        address.append(data6)
        
        data7 = content.findAll('div',{"class":"job-detail"})[0].get_text().replace('\xa0','') #具体描述
        describe.append(data7)
    
for i in range(1,31):
    url = "https://www.lagou.com/zhaopin/Python/"+str(i)+"/?filterOption=3&sid=262751283f774b78b55ac6639e52d6ea"
    if i%5==0:
        time.sleep(20)
    print('正在采集第%s个页面'%str(i))
    scraping(url)
print('所有页面数据采集完成！')    

file = open('lagouwang.csv','w',encoding='utf-8-sig')  #创建CSV文件
writer = csv.writer(file)
writer.writerow(['job','company','salary','experience','education','address'])

f = open('职位描述.txt','w') #创建txt文件

table = 'lagouwang_python'      #数据库中表的名称
keys = ('zhiwei,gongsi,gongzi,jingyan,xueli,dizhi')
values = ','.join(['%s']*6)
print('正在导入数据……')
for i in range(len(job)):
    try:
        zhiwei = job[i]
        gongsi = company[i]
        gongzi = salary[i]
        jingyan = experience[i]  
        xueli = education[i]
        dizhi = address[i]
        data =(zhiwei,gongsi,gongzi,jingyan,xueli,dizhi)
        miaoshu = describe[i]
        writer.writerow([zhiwei,gongsi,gongzi,jingyan,xueli,dizhi]) #将数据写入csv文件
        f.write(miaoshu) #将职位描述写入txt文件
        sql = 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(table=table,keys=keys,values=values)
        cur.execute(sql,data) #将数据写入数据库
        db.commit()
        
    except Exception as e:
        print('出现异常：' + str(e))
        continue
file.close() #关闭文件接口
f.close()
cur.close()
db.close()
print('数据导入完成！')    
driver.close() #关闭浏览器