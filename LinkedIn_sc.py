#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from bs4 import BeautifulSoup
from urllib.error import HTTPError,URLError
import requests
import pandas as pd
import json
import time


# In[ ]:


import pyodbc


# In[ ]:


def ifNone(value,List):
    for i in range(len(List)):
        try:
            List[i].append(value[i].text.replace('\n','').strip())
        except IndexError:
            List[i].append(None)
    return value,List

def take_info(soup,Title, Company, Location, Time_Published, EmploymentType, Industry, Level, JobFunc):
    jobs=soup.find_all('li')
    for index,job in enumerate(jobs):
        headers={}
        name=(job.find('h3',{'class':'base-search-card__title'}))
        c=job.find('h4',class_='base-search-card__subtitle')
        l=job.find('span',class_='job-search-card__location')
        try:
            link=job.find('a').get('href')#,{'class':'base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]'}
        except AttributeError:
            print(job)
        r=requests.request('get',link,headers=headers)
        t=3
        while r.status_code==429:
            time.sleep(t)
            r=requests.request('get',link,headers=headers)
            print('Sleep for ', t, ' Sec')
            print(r)
            t+=2
            #time.sleep(int(r.headers['Retry-After']))
            #html_post=requests.get(link)
        post=BeautifulSoup(r.text,'lxml')
        Info=post.find_all('span',{'class':'description__job-criteria-text description__job-criteria-text--criteria'})
        if 1>len(Info)<4:
            print('Info Is: \n',Info)
        try: 
            Dict=json.loads(post.find('script',{'type':'application/ld+json'}).string)
        except AttributeError:
            Time_Published.append(None)
        else:
            Time_Published.append(pd.to_datetime(Dict['datePosted']))
        ifNone(Info,[Level,EmploymentType,JobFunc,Industry])
        Location.append(l.text.replace('\n','').strip())
        Company.append(c.text.replace('\n','').strip())
        Title.append(name.text.replace('\n','').strip())
        #ifNone(l,Location)
        #ifNone(c,Company)
        #ifNone(name,Title)
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server={Server Name};'
                      'Database={Database Name};'
                      'Trusted_Connection=yes;')
        cursor = conn.cursor()

        query='''
        INSERT INTO Jobs VALUES (?,?,?,?,?,?,?,?)
        '''
        data=[Title[-1],Company[-1],Location[-1],Time_Published[-1],EmploymentType[-1],Industry[-1],Level[-1],JobFunc[-1]]
        cursor.execute(query,data)
       
        conn.commit()
        r.close()

    return Title, Company, Location, Time_Published, EmploymentType, Industry, Level, JobFunc

def func(response,num_jobs,page_count):
    Title=[]
    Company=[]
    Location=[]
    Time_Published=[]
    EmploymentType=[]
    Industry=[]
    Level=[]
    JobFunc=[]
    while page_count<=num_jobs:
        headers={}
        url = f'https://il.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=&location=Israel&locationId=&geoId=101620260&f_TPR=r86400&start={page_count}' 
        response=requests.request('get',url,headers=headers)
        t=3
        while response.status_code==429:
            time.sleep(t)
            response=requests.request('get',url,headers=headers)
            print('Sleep for ', t, ' Sec')
            t+=2 
        soup=BeautifulSoup(response.text,'lxml')
        Title, Company, Location, Time_Published, EmploymentType, Industry, Level, JobFunc=take_info(soup,Title, Company, Location, Time_Published, EmploymentType, Industry, Level, JobFunc)
        response.close()
        print(page_count)
        page_count += 25
    df=pd.DataFrame({'Title':Title,'Company':Company,'Seniority Level':Level,'Location':Location,'Time':Time_Published, 
                          'Industry':Industry,'Job Function':JobFunc, 'Employment Type':EmploymentType},dtype='string')
    


    return soup, df


# In[ ]:




try:
    response=requests.get('https://il.linkedin.com/jobs/search?location=Israel&geoId=101620260&f_TPR=r86400&currentJobId=3490172536&position=1&pageNum=0')
    num_jobs=int(BeautifulSoup(response.text,'lxml').find('span',{'class':'results-context-header__job-count'}).text)   
except (HTTPError,URLError) as error:
    print (error)
else:
    res=response
    try:
        soup, df=func(response,num_jobs,0)
    except (HTTPError,URLError) as error:
        print('0')

