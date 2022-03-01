import pandas as pd
import requests  
import json
import time
import os
import pdfplumber as pdf
import re
import numpy as np

##爬虫函数，用的post方法向api传参
def SZ_spyder(flv):
    lp_url='http://www.cncapital.net/szsa/archives/list/1.0' #api
    headers={
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding":"gzip, deflate",
            "Accept-Language":"zh-CN,zh;q=0.9",
            "Host":"www.cncapital.net",
            "User-Agent":"xxx"
        }## 用自己的User-Agent

    data = {'typeid':18,'pagesize':40,'pageno':1}
    req = requests.post(lp_url,data=data,headers=headers)
    time.sleep(2)
    r = json.loads(req.text)
    clist=r['data']
    tlist=[]
    for i in clist:
        id = i['id']
        title=i['title']
        data2 = {'id':id}
        if title not in flv:
            tlist.append(title+'.pdf')
            req2 = requests.post(lp_url,data=data2,headers=headers)
            time.sleep(1)
            r2 = json.loads(req2.text)
            fff = r2['attachment'][0]['url']
            print(fff)
            pdf_url = 'http:'+fff
            file = requests.get(pdf_url,headers=headers)
            time.sleep(1)
            file_path='D:/'+title+'.pdf'
            f = open(file_path,'wb')
            f.write(file.content)
            f.close()
        else:
            continue
    return tlist

##读取pdf    
def read_pdf(p_num,pf,mm):
    df_trd = pd.DataFrame(columns=['dpm_nm','value'])
    df_inc = pd.DataFrame(columns=['dpm_nm','value'])
    df_prf = pd.DataFrame(columns=['dpm_nm','value'])
    pnn=p_num-1
    pn_list=list(range(p_num))
    if int(mm)>=20210601: ##20210601之后,当月值和当月累计值调换了位置
        ll = [0,3,4]
    else :
        ll = [0,1,2]
    for j in pn_list :
        pp = pf.pages[j]
        t = pp.extract_table()
        dp = pd.DataFrame(t[1:],columns=t[0])
        if len(list(dp.columns))>3:
            dpp = dp.iloc[:,ll].set_axis(['dpm_nm','当月排序','value'],axis='columns')
            dpp['dt']=mm
        else:
            dpp = dp.iloc[:,:3].set_axis(['当月排序','dpm_nm','value'],axis='columns')
            dpp['dt']=mm
        fdf = dpp[['dpm_nm','value','dt']]
        df_trd = pd.concat([df_trd,fdf],axis=0)
        pnn=pnn-1
        print('交易额:第'+str(j)+'页')
        if dpp['dpm_nm'].iloc[-1]!='合计' and dpp['dpm_nm'].iloc[-1]!='':
            continue
        else:
            k=j+1
            for k in pn_list[j+1:]:
                pp = pf.pages[k]
                t = pp.extract_table()
                dp = pd.DataFrame(t[1:],columns=t[0])
                if len(list(dp.columns))>3:
                    dpp = dp.iloc[:,:3].set_axis(['dpm_nm','当月排序','value'],axis='columns')
                    dpp['dt']=mm
                else:
                    dpp = dp.iloc[:,:3].set_axis(['当月排序','dpm_nm','value'],axis='columns')
                    dpp['dt']=mm
                fdf = dpp[['dpm_nm','value','dt']]
                df_inc = pd.concat([df_inc,fdf],axis=0)
                pnn=pnn-1
                print('收入:第'+str(k)+'页')
                if dpp['dpm_nm'].iloc[-1]!='合计' and dpp['dpm_nm'].iloc[-1]!='':
                    continue
                else:
                    l=k+1
                    for l in pn_list[k+1:]:
                        pp = pf.pages[l]
                        t = pp.extract_table()
                        dp = pd.DataFrame(t[1:],columns=t[0])
                        if len(list(dp.columns))>3:
                            dpp = dp.iloc[:,:3].set_axis(['dpm_nm','当月排序','value'],axis='columns')
                            dpp['dt']=mm
                            fdf = dpp[['dpm_nm','value','dt']]
                            df_prf = pd.concat([df_prf,fdf],axis=0)
                            pnn=pnn-1
                            print('净利润:第'+str(l)+'页')
                        elif len(list(dp.columns))>1:
                            dpp = dp.iloc[:,:3].set_axis(['当月排序','dpm_nm','value'],axis='columns')
                            dpp['dt']=mm
                            fdf = dpp[['dpm_nm','value','dt']]
                            df_prf = pd.concat([df_prf,fdf],axis=0)
                            pnn=pnn-1
                            print('净利润:第'+str(l)+'页')
                        else:
                            break
                    break
            break    
    return df_trd,df_inc,df_prf  

##pd.ExcelWriter可以写进excel不同的sheet
def write_excel(df_trd,df_inc,df_prf):
    writer = pd.ExcelWriter('D:/merge.xlsx')
    df_prf.to_excel(writer,sheet_name='净利润',index=False)
    df_inc.to_excel(writer,sheet_name='营收',index=False)
    df_trd.to_excel(writer,sheet_name='交易量',index=False)
    writer.save()
    writer.close()

#只解析pdf
def parse_only():
    work_path = 'D:/'
    for root, dirs, files in os.walk(work_path):
        fl=files
    flv = [x.replace('.pdf','') for x in fl]  ###这个列表记录了文件夹内已有的文件，传回爬虫函数，控制不重复爬取
    for root, dirs, files in os.walk(work_path):
        flst=files
    set1 = set(fl)
    set2 = set(flst)
    tg = list(set1^set2)
    ###剔除年度汇总数据，建立文件名列表file_list
    for l in flst:  ##这里tg就是去重，flst就是文件夹里面所有pdf都解析
        if '年度' in l:
            flst.remove(l)
        else:
            continue
    file_list=[work_path+f for f in flst if 'pdf' in f]
    df1 = pd.DataFrame(columns=['dpm_nm','value','dt'])
    df2 = pd.DataFrame(columns=['dpm_nm','value','dt'])
    df3 = pd.DataFrame(columns=['dpm_nm','value','dt'])
    ###循环调用解析函数
    for i in file_list:  #file_list[1:]是因为第一个文件是merge.xlsx
        print('正在解析: '+ i)
        pf = pdf.open(i)
        p_num = len(pf.pages)
        year = re.findall('[0-9]+',i)[0]
        month = re.findall('[0-9]+',i)[1]
        if len(month)<2:
            month='0'+month
        mm = year+month+'01' 
        df_t,df_i,df_p = read_pdf(p_num,pf,mm)
        df1 = pd.concat([df1,df_t],axis=0)
        df2 = pd.concat([df2,df_i],axis=0)
        df3 = pd.concat([df3,df_p],axis=0)
    ###写入excel
    df1['dt'] = df1['dt'].astype('int')
    df2['dt'] = df2['dt'].astype('int')
    conditions1 = [int(df1['dt'])<20210701,int(df1['dt'])>=20210701] ##2021年7月以前，pdf展示顺序是交易额-营收-利润，2021年7月开始是营收-交易额-利润
    conditions2 = [int(df2['dt'])<20210701,int(df2['dt'])>=20210701]
    choice1 = ['交易额','营收']
    choice2 = ['营收','交易额']
    df1['flag']=np.select(conditions1,choice1)
    df2['flag']=np.select(conditions2,choice2)
    df3['flag']='净利润'
    cc = pd.concat([df1,df2],axis=0)
    fdf = pd.concat([cc,df3],axis=0)
    fdf['region']='深圳'
    fdf['value'] = fdf['value'].replace('-','')
    fdf.to_excel('D:/merge1.xlsx',index=False)    



##爬虫+解析pdf
def run() :
    work_path = 'D:/'
    for root, dirs, files in os.walk(work_path):
        fl=files
    flv = [x.replace('.pdf','') for x in fl]  ###这个列表记录了文件夹内已有的文件，传回爬虫函数，控制不重复爬取
    tlist = SZ_spyder(flv)   ###爬虫
    print(tlist)
    if len(tlist)>0 :
        ###pdf解析
        flst=[] #获得爬取后的pdf文件路径
        for root, dirs, files in os.walk(work_path):
            flst=files
        set1 = set(fl)
        set2 = set(flst)
        tg = list(set1^set2)
        ###剔除年度汇总数据，建立文件名列表file_list
        for l in tg: 
            if '年度' in l:
                tg.remove(l)
            else:
                continue
        file_list=[work_path+f for f in tg if 'pdf' in f]
        df1 = pd.DataFrame(columns=['dpm_nm','value','dt'])
        df2 = pd.DataFrame(columns=['dpm_nm','value','dt'])
        df3 = pd.DataFrame(columns=['dpm_nm','value','dt'])
        ###循环调用解析函数
        for i in file_list:  #file_list[1:]是因为第一个文件是merge.xlsx
            print('正在解析: '+ i)
            pf = pdf.open(i)
            p_num = len(pf.pages)
            year = re.findall('[0-9]+',i)[0]
            month = re.findall('[0-9]+',i)[1]
            if len(month)<2:
                month='0'+month
            mm = year+month+'01'  
            df_t,df_i,df_p = read_pdf(p_num,pf,mm)
            df1 = pd.concat([df1,df_t],axis=0)
            df2 = pd.concat([df2,df_i],axis=0)
            df3 = pd.concat([df3,df_p],axis=0)
        ###写入excel
        #这里np.select方法可以了解一下，有条件地生成标签，挺好用的，pandas有类似的where方法
        conditions = [int(df1['dt'])<20210701,int(df1['dt']>=20210701)] ##2021年7月以前，pdf展示顺序是交易额-营收-利润，2021年7月开始是营收-交易额-利润
        choice1 = ['交易额','营收']
        choice2 = ['营收','交易额']
        df1['flag']=np.select(conditions,choice1)
        df2['flag']=np.select(conditions,choice2)
        df3['flag']='净利润'

        cc = pd.concat([df1,df2],axis=0)
        fdf = pd.concat([cc,df3],axis=0)
        fdf['region']='深圳'
        fdf.to_excel('D:/merge.xlsx',index=False)
        #write_excel(df1,df2,df3)
    else:
        print("无新增数据")

if __name__ == '__main__': 
    run()
    #parse_only()


