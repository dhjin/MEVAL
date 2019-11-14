import os
import re
import sys
import glob
import argparse
import json
import difflib
import pandas as pd
import pymysql
import pickle
import requests
import csv
import core

#################################################################################################################
#
#  autotest--
#           - getResults: 데이터 폴더에 있는 원본을 2개의 ES서버에 요청하여 결과 비교를 해냄. 
#           - mergeRes: 
#           - 
#
#################################################################################################################
CFG = { 
    # elastic
    'es_index'         : 'products',
    'es_analyzer'      : 'nori_custom'
    #'es_analyzer'      : 'standard'
}


'''
ES에서 얻은 결과 앤트리에서 토큰들을 추출하기 위한 방법, 
(1) 
'''
def getTokens(ent):
    syn = {}
    token_str = []
    # --INSERT Synonyms 
    for x in ent:
        if x and x['type']=='SYNONYM':
            if x['position'] in syn:
                if 'positionLength' in x:
                    syn[x['position']] += [(x['token'],x['position'],x['positionLength'])]
                else:
                    syn[x['position']] += [(x['token'],x['position'],1)]
            else:
                if 'positionLength' in x:
                    syn[x['position']] = [(x['token'],x['position'],x['positionLength'])]
                else:
                    syn[x['position']] = [(x['token'],x['position'],1)]
    # --Synonym mapping
    for x in ent:
        rs = ""
        if x:
            if x['type']=='word':
                rs += x['token']+"\t\t"+str(x['position'])
                token_str.append(x['token'])
                if x['position'] in syn:
                    rs += "\t"+ str(syn[x['position']])
                #print(rs)
    #print(" ".join(token_str))
    return " ".join(token_str), syn

def getOne(i,rs,penulty):
    #분석 결과를 하나씩 돌아..
    for z in rs:
        if z:
            tp = z['type']
            ps = z['position']
            if tp=='word' and ps==i+penulty:
                if "positionLength" in z:
                    return z['token'], z['positionLength']-1
                return z['token'], 0
    return None

'''
mode = 0: diffing 결과만
mode = 1: 전체 분석 결과와 함께
'''
def getESResult(agenda, resOne, t, t1):
    # 스트링 안에 Alphanumeric이 하나라도 있을 때.
    #p = re.compile('[A-Za-z0-9&\+#]+')
    #print(t+"\t"+t1+"\t"+t2)
    clist = []
    for a in agenda:
        #첫번째 토큰 세트
        a1 = a[0]
        for b in a1:
            print(b)

        #if True:
        #    clist.append((t,t1,t2,cm[0],cm[1],alpha))

    return clist


'''
리스트를 맨 마지막에 집어 넣고 값을 받아.
'''
def getES(contents, res, cycle):
    source = contents[cycle]#['_source']['deal_name']
    t1, s1 = getTokens(res)
    # 디프 결과 알맞게 출력..
    clst = t1.split(" ")
    return clst
'''


'''
def getResults(cat_names,folder,port1):
    for ct in cat_names:
        # ct 카테고리명
        contents_ = {}
        idx = 0
        f_res = open("./res/test_res_"+ct+".txt","w")
        print(ct)
        f = open(folder+ct+".txt","r",encoding="utf-8")
        #상품 리스트 완성.
        for x in f:
            ln = x.rstrip("\n")
            ln = ln.strip(" ")
            #contents.append(ln)
            contents_[idx] = ln
            ln = ln.replace('"','``')
            try:
                tl = core.query_analysis(port1,CFG["es_index"],CFG["es_analyzer"],ln.replace("'","\''").replace('"','\"'))
            except:
                print(ln)
            res_list = getES(contents_, tl, idx)
            joined = " ".join(res_list)
            for r in res_list:
                f_res.write(ct+"\t"+ln+"\t"+joined+"\t"+r+"\n")
            idx+=1 

        f_res.close()
        f.close()



'''
개요: 'folder'안에 들어 있는 cat_names 이름의 파일을 하나씩 읽어 DB에 상품명을 넣음.
-cat_names[]: 카테고리명들=파일명들
-folder: 대상폴더 path
port1: 대상 ES기기 포트,
-db_password: 대상 DB 패스워드(유저명은 'root')
-dataset: 대상 데이터 세트 (예> P1, P2)
'''
def initDB(cat_names,folder,db_password, dataset):
    #쿼리들
    #<---상품명 테이블에 상품명 집어넣기--->
    sql_i_p = """INSERT INTO product(did,pid,text,cid) values(%s,%s,%s,%s)"""
    #<---토큰 번호 이름 세팅--->
    sql_i_t = """INSERT INTO tokens(pid,ord,tid,start,end) values(%s,%s,%s,%s,%s)"""
    #<---토큰-번호 매핑 세팅-->
    sql_i_tm = """INSERT INTO tokenMappings(tid,token) values(%s,%s)"""
    #-------------------------------------
    #데이터베이스 연결
    print("CONNECT DB")
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    #사전세팅
    # cdic: {"식품생활":1,...} 
    # azdic: {"N7.2_T8":1,...}
    # ddic: {"P1":1,...}
    # tdic: {111:"분석형"}
    # adic: {7833:"분석 형"}
    # pdic: {1:"분석형 몇가지..",..}
    print("GET ALL DICS...")
    cdic, azdic, ddic, tdic, adic, pdic, udic = getParameterDics(db_password)
    #-------------------------------------
    #중복 엔트리 제거.
    #pdic_rev2 = {}
    #(0)데이터셋 확인하여 이름 삽입.
    ddic = updateDatasetVersion(db_password,dataset)
    #-------------------------------------
    #TEMPORARY "token-tid" DIC
    tedic = {}

    #문장 최후 엔트리 번호
    curs.execute("select max(pid) from product")
    sennum = curs.fetchall()[0][0]
    #토큰 최후 엔트리 번호
    curs.execute("select max(tid) from tokenMappings")
    tedicnum = curs.fetchall()[0][0]

    for ct in cat_names:
        # ct 카테고리명
        #-------------------------------------
        #(1)해당 카테고리명이 데이터 베이스에 있는지 확인.
        cdic = updateCategoryVersion(db_password,ct)
        #-------------------------------------
        #(0)데이터셋 확인하여 이름 삽입.
        sql = "select token,tid from tokenMappings"
        curs.execute(sql)
        todic = dict(curs.fetchall())
        #-------------------------------------
        # - 파일을 가지고 상품 리스트 돌기.( 파일은 1라인당 1상품명으로 이루어짐)
        f = open(folder+ct+".txt","r",encoding="utf-8")
        print("READ FILE..")
        for x in f:
            #dup_sql = "select token,tid from tokens WHERE"
            ln = x.rstrip("\n")
            ln = ln.replace(r'\t'," ")
            ln = ln.strip(" ")
            idx = 0
            #에러 나오는 표현 바꾸기.
            ln = ln.replace('"','“')
            ln = ln.replace("'","`")
            if len(ln) >250:
                print("Too long..",ln)
                continue
            '''
            if ln in pdic_rev2:
                pdic_rev2[ln] = 0
                print("중복되는 엔트리",ln)
                continue
            '''
            if ln:
                # - 상품 명 product 테이블에 넣기 --> 해당 파일에 있는 상품명들이 최초로 들어감. -- !!!!! 문장번호는 고유하다.
                sennum+=1
                curs.execute(sql_i_p,(ddic[dataset],sennum,ln,cdic[ct]))
                conn.commit()
                #라인을 갈라 원본 토큰 만들기
                tokens = re.split(r'[\[\]\(\)\{\}\/ ]',ln)
                start = 0
                end = 0
                # 토큰을 하나씩 꺼내서 DB에 넣는다.
                for t in tokens:
                    start = end
                    end = start+len(t)
                    if not start==end: # 길이가 0인 스트링이 토큰일 때, 넘긴다.
                        # 번호-토큰 매핑 사전에 잇다면.
                        if t in todic or t in tedic:
                            pass
                        else:
                            # 토큰-매핑 사전에 [0:"토큰"]을 집어넣는 조작.
                            tedicnum+=1
                            #curs.execute("select token,tid from tokenMappings")
                            tedic[t] = tedicnum
                            #토큰 사전의 새로 들어온 토큰 값을 0으로 설정... 앞으로 안쓸 거라 괜찮.
                            #tdic[t] = 0
                        #tokens 에 엔트리 추가.// tmap에 
                        if t in tedic:
                            tid = tedic[t]
                        else:
                            tid = todic[t]
                        # 토큰 삽입.
                        curs.execute(sql_i_t,(sennum,idx,str(tid),start,end))    
                        conn.commit()
                        idx+=1
                    end += 1
                if sennum%1000==0:
                    print(sennum)
        #-------------------------------------
        f.close()
    for te in tedic:
        curs.execute(sql_i_tm,(tedic[te],te))
        conn.commit()
    conn.close()



def checkNinsert(db_password,did):
    #쿼리들
    #<---토큰-번호 매핑 세팅-->
    sql_i_tm = """INSERT INTO tokenMappings(tid,token) values(%s,%s)"""
    #-------------------------------------
    #데이터베이스 연결
    print("CONNECT DB")
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    curs.execute("select pid,text from product where did="+str(did))
    sentences = dict(curs.fetchall())

    sql = "select token,tid from tokenMappings"
    curs.execute(sql)
    todic = dict(curs.fetchall())
    tedic = {}

    for x in sentences:
        sen = sentences[x]
        tokens = re.split(r'[\[\]\(\)\{\}\/ ]',sen)
        # 1 문장의 분석 결과에서 토큰 아이디만 추출.
        curs.execute("select aid,tid from result where pid="+str(x))
        tidz = dict(curs.fetchall())
        # 2. 문장의 분석 결과에서 분석토큰-분석 아이디 매핑 추출.
        curs.execute("select res,aid from analysis where aid in (select aid from result where pid="+str(x)+" and tid>147956)")
        results = dict(curs.fetchall())

        for t in results:
            trim = t.replace(" ","")
            if trim in tokens:
                if trim in todic:
                    pass
                elif trim in tedic:
                    pass
                else:
                    tedic[trim] = tidz[results[t]]
                    print("INSERTED: ",tedic[trim],t,trim)
                    #매핑할 토큰 삽입
                    curs.execute(sql_i_tm,(tedic[trim],trim))
                    conn.commit()
    conn.close()


# category 버전 세팅
def updateCategoryVersion(db_password, cat):
    #<---카테고리에 번호 매겨서 이름 집어넣기--->
    sql_i_cat = """INSERT INTO category(name) values(%s)"""
    # 1.데이터베이스 연결
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    curs.execute("select name,cid from category")
    cdic = dict(curs.fetchall())
    if cat in cdic:
        pass
    else:
        curs.execute(sql_i_cat,(cat))
        conn.commit()
        curs.execute("select name,did from category")
        cdic = dict(curs.fetchall())
    return cdic

# dataset 버전 세팅
def updateDatasetVersion(db_password, dset):
    sql_i_dat = """INSERT INTO dataset(name) values(%s)"""
    # 1.데이터베이스 연결
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    curs.execute("select name,did from dataset")
    ddic = dict(curs.fetchall())
    if dset in ddic:
        pass
    else:
        curs.execute(sql_i_dat,(dset))
        conn.commit()
        curs.execute("select name,did from dataset")
        ddic = dict(curs.fetchall())
    return ddic

# 형태분석기 버전 세팅
def updateAnalyzerVersion(db_password,n_ver):
    sql_i_az = """INSERT INTO analyzer(name) values(%s)"""
    # 1.데이터베이스 연결
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    curs.execute("select name,aid from analyzer")
    ndic = dict(curs.fetchall())
    if n_ver in ndic:
        pass
    else:
        curs.execute(sql_i_az,(n_ver))
        conn.commit()
        curs.execute("select name,aid from analyzer")
        ndic = dict(curs.fetchall())
    return ndic

def updateUserdicVersion(db_password,u_ver):
    sql_i_u = """INSERT INTO userdic(date) values(%s)"""
    # 1.데이터베이스 연결
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    curs.execute("select date,uid from userdic")
    udic = dict(curs.fetchall())
    if u_ver in udic:
        pass
    else:
        curs.execute(sql_i_u,(u_ver))
        conn.commit()
        curs.execute("select date,uid from userdic")
        udic = dict(curs.fetchall())
    return udic

'''
'''
def getTokenDic(db_password,did):
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    curs.execute("select min(pid) from product where did="+str(did))
    start = curs.fetchall()
    curs.execute("select max(pid) from product where did="+str(did))
    end = curs.fetchall()
    sql = "select * from tokens WHERE pid>"+str(start[0][0]-1)+" and pid<"+str(end[0][0]+1)
    curs.execute(sql)
    tokens = curs.fetchall()
    tokendic = {}
    for t in tokens:
        if t[1] in tokendic:
            l = tokendic[t[1]]
            tokendic[t[1]] = l+[t]
        else:
            tokendic[t[1]] = [t]
    return tokendic
'''
-dset: 테스트할 데이터셋
-port: 접근할 ES 포트.
-nori_ver: 개발한 노리분석기 버전
-udic_ver: 구축한 확장사전 버전
'''
def getSnapshot(dset, db_password, port, n_ver, u_ver, start,cut):
    body = {"analyzer":"nori_custom","text":"","explain":"false"}
    #auth('anal-test','search1!')
    #<---토큰-번호 매핑 세팅-->
    sql_i_r = """INSERT INTO result(pid,ttid,cid,tid,did,aid,ver,uid) values(%s,%s,%s,%s,%s,%s,%s,%s)"""
    sql_i_an = """INSERT INTO analysis(res,aid) values(%s,%s)"""
    #-------
    # 1.데이터베이스 연결
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    #커저 세팅
    curs = conn.cursor()
    #사전세팅
    # cdic: {"식품생활":1,...} 
    # azdic: {"N7.2_T8":1,...}
    # ddic: {"P1":1,...}
    # tdic: {111:"분석형"}
    # adic: {7833:"분석 형"}
    # pdic: {1:"분석형 몇가지..",..}
    print("GET ALL DICS...")
    #cdic, azdic, ddic, tdic, adic, pdic, udic = getParameterDics(db_password)
    #데이터셋 버전 세팅
    ddic = updateDatasetVersion(db_password,dset)

    # 2.상품 목록 세팅 by dataset idx.
    sql = "select pid,text from product WHERE did="+str(ddic[dset])#+" and pid<"+str(cut)+" and pid>="+str(start)

    curs.execute(sql)
    pdic = dict(curs.fetchall())

    #형태 분석기 버전 세팅
    ndic = updateAnalyzerVersion(db_password,n_ver)
    # 유저사전 버전 세팅
    udic = updateUserdicVersion(db_password,u_ver)

    # 3. 데이터 셋 번호를 넣고, 그에 해당하는 상품 인덱스-카테고리 사전 획득,
    sql = "select pid,cid from product WHERE did="+str(ddic[dset])
    curs.execute(sql)
    product_category = dict(curs.fetchall())

    # 4. 토큰번호-토큰 매핑 가져오기.
    sql = "select tid,token from tokenMappings"
    curs.execute(sql)
    tm = dict(curs.fetchall())

    # 분석폼-분석아이디.
    curs.execute("select res,aid from analysis")
    adic = dict(curs.fetchall())    

    tokens = getTokenDic(db_password,ddic[dset])

    # 분석형의 DB pendings
    adic_pending = {}

    #현재 분석폼 사전의 최종 인덱스,
    curs.execute("select max(aid) from analysis")
    adic_last_idx = curs.fetchall()[0][0]

    # 이미 해당 상품의 분석 결과가 들어와 있다면 제외한다.
    curs.execute("select pid,aid from result where did="+str(ddic[dset])+" AND ver="+str(ndic[n_ver])+" AND uid="+str(udic[u_ver]))
    dudic = dict(curs.fetchall())

    # 상품 목록 돌기
    result_dic = {}
    result_pending = {}

    print("START!")
    for i in pdic:
        # 이미 처리한 것인지 체크.(동일한 데이터 세트, 동일한 버전의 분석기에서, 동일한 문장이 result 테이블에 존재할 때:)
        if i in dudic:
            print("이미 처리한 데이터.",dset," for ",n_ver)
            continue
        #    break
        # 번호-텍스트
        txt = pdic[i]
        #변환 처리를 위한 변형.
        #txt = txt.replace(" ","_")
        body["text"] = txt
        # ES 머신에 텍스트 집어넣기, res 리스트에 결과 하나씩 집어넣기.
        try:
            tl = core.query_analysis(port,CFG["es_index"],CFG["es_analyzer"],txt.replace("'","`").replace('"','“'))
        except:
            print("ERROR:  "+txt+"  remove..this line..")
            continue
        try:
            ts, om = core.makeTokenstream(tl)
        except:
            print("ERROR: " +txt)
            continue
        #print(ts,om)

        # 최종 원본-분석형 매핑 결과물
        result = core.getMapping(i,tokens[i], tm, ts['word'], om, db_password)
        
        # 분석 결과 집어넣기
        # i번째 문장에 있는 토큰들 가져오기
        result_dic[i] = result
        #print("snum \t tokn \t cat \t t_id \t anal \t n_ver \t u_ver")
        #원형 토큰 사전에서 한토큰씩 고려..
        if i%1000==0 and i>0:
            #break
            print(i,txt)
            curs.execute("select max(rid) from result")
            rid_last_idx = curs.fetchall()[0][0]
            for r in result_dic:
                result = result_dic[r]
                toks = tokens[r]
                for t in toks:
                    # 문장번호 / 토큰 고유번호 /카테고리번호 / 토큰사전넘버 / 데이터셋번호 / 노리버전 / 사용자사전업데이트 날짜 ..
                    #print(i,"\t",t[0],"\t",product_category[i],"\t",t[3],"\t",adic[result[t[2]]],"\t",ndic[n_ver],"\t",0)
                    if result[t[2]] in adic_pending: # 펜딩 사전에 분석형이 있을 시, 
                        aid = adic_pending[result[t[2]]]
                    elif result[t[2]] in adic:       # 기존 분석 사전에 분석형이 있을 시.
                        aid = adic[result[t[2]]]
                    else:                            # 아무데도 분석형이 없을 시.
                        adic_last_idx+=1
                        adic_pending[result[t[2]]] = adic_last_idx
                        aid = adic_pending[result[t[2]]]
                    # Result ID 값 증가.
                    rid_last_idx+=1
                    record = (r, t[0], product_category[r], t[3], ddic[dset], aid, ndic[n_ver], udic[u_ver])
                    result_pending[rid_last_idx] = record
                # 결과값 데이터 베이스에 추가.
            
            #print("분석형 사전에 신규 분석형 삽입")
            for an in adic_pending:
                if not an in adic:
                    print(adic_pending[an],an)
                    curs.execute(sql_i_an,(an,adic_pending[an]))
                    conn.commit()
            adic_pending = {}
            # 분석폼-분석아이디 재로딩.
            curs.execute("select res,aid from analysis")
            adic = dict(curs.fetchall())    
            #print("걀과 값 삽입!")
            for r in result_pending:
                res = result_pending[r]
                curs.execute(sql_i_r,(res[0], res[1], res[2], res[3], res[4], res[5], res[6], res[7]))
                conn.commit()
            result_pending = {}
            result_dic = {}
    # analysis 사전 불러와서 업데이트
    # 기존 analysis 사전에 없는 엔트리이면 집어넣는다.
    conn.close()

'''
result set에 특정 데이터셋, 특정 분석기버전, 특정 유저사전 버전의 분석 데이터가 이미 있을 시 false 를 리턴한다.
'''
def duplicate(db_password, dataset, n_ver, u_ver, product_id, ndic, ddic, udic):
    if (dataset in ddic) and (n_ver in ndic) and (u_ver in udic):
        conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
        curs = conn.cursor()
        curs.execute("select pid,aid from result where did='"+str(ddic[dataset])+"' AND ver='"+str(ndic[n_ver])+"' AND pid='"+str(product_id)+"'")
        pdic = dict(curs.fetchall())
        conn.close()
        return (product_id in pdic) # maybe True
    return False


def getParameterDics(db_password):
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    curs = conn.cursor()
    #카테고리 사전
    curs.execute("select name,cid from category")
    cdic = dict(curs.fetchall())

    #분석기 버전명 사전
    curs.execute("select name,aid from analyzer")
    azdic = dict(curs.fetchall())

    #데이터셋 이름 사전
    curs.execute("select name,did from dataset")
    ddic = dict(curs.fetchall())

    #토큰명 사전
    curs.execute("select tid,token from tokenMappings")
    tdic = dict(curs.fetchall())

    #분석 토큰 사전
    curs.execute("select aid,res from analysis")
    adic = dict(curs.fetchall())

    #분석 토큰 사전
    curs.execute("select pid,text from product")
    pdic = dict(curs.fetchall())

    #분석 토큰 사전
    curs.execute("select date,uid from userdic")
    udic = dict(curs.fetchall())

    conn.close()
    return cdic, azdic, ddic, tdic, adic, pdic, udic

'''
데이터 프레임을 사전으로 바꿔줌. 
'''
def dataframe2dict(data,k_name,v_name):
    kset = data.to_dict()[k_name]
    vset = data.to_dict()[v_name]
    outs = {}
    for i in kset:
        outs[kset[i]] = vset[i]
    return outs


def getDics():
    uz = pd.read_csv("./db/userdic.csv")
    udic = dataframe2dict(uz,"date","uid")
    cz = pd.read_csv("./db/category.csv")
    cdic = dataframe2dict(cz,"name","cid")
    dz = pd.read_csv("./db/dataset.csv")
    ddic = dataframe2dict(dz,"name","did")
    vz = pd.read_csv("./db/analyzer.csv")
    vdic = dataframe2dict(vz,"name","aid")
    tz = pd.read_csv("./db/tokenMappings.csv")
    tmap = dataframe2dict(tz,"tid","token")
    az = pd.read_csv("./db/analysis.csv")
    amap = dataframe2dict(az,"aid","res")
    return udic,cdic,ddic,vdic,tmap,amap


'''
~~~~ 영향 평가 ~~~~~~
(1) 모델 1과 모델 2 사이에 상이한 모든 토큰을 소환..

ARG
0. DB 패스워드
1. AS-IS 분석기 버전
2. TO-BE 분석기 버전
3. user사전 버전 --> (as-is와 to-be가 같아야 한다.)
4. 데이터 세트 

'''
def evaluation(asis, tobe, u_ver, dset):
    print("Data load..")
    #각종 사전들 모아오기.
    udic, cdic, ddic, vdic, tmap, amap = getDics()
    # 2.비교셑 모아오기(AS_IS)
    pz = pd.read_csv("./db/product.csv")
    pdic = dataframe2dict(pz,"pid","text")
    p_ = pz.query('did=='+str(ddic[dset]))
    # 데이터셋 "A"에서의 상품 리스트
    plist = p_['pid']#.sample(n=3, random_state=1)

    res = pd.read_csv("./db/result.csv")
    res = res.query('did=='+str(ddic[dset]))

    f = open("./01.ImprovementTEST/"+dset+"_"+asis+"_"+tobe+".txt","w")

    print("\t토큰번호\t대상토큰\t이전버전분석\t비교버전분석")
    if (dset in ddic) and (asis in vdic) and (tobe in vdic): # 데이터셋 노리 이전/다음 버전이 다 등록된 것일 때.
        aiz = res.query('ver=='+str(vdic[asis]))
        tbz = res.query('ver=='+str(vdic[tobe]))
        aiz = aiz.query('uid=='+str(udic[int(u_ver)]))
        tbz = tbz.query('uid=='+str(udic[int(u_ver)]))
        total = len(tbz)

        diff = 0
        error = 0
        for j in plist:
            ai = aiz.query('pid=='+str(j))
            tb = tbz.query('pid=='+str(j))
            ai_t = ai['tid'].to_list()
            tb_t = tb['tid'].to_list()
            ai_a = ai['aid'].to_list()
            tb_a = tb['aid'].to_list()
            if len(ai_t)==0 or len(tb_t)==0:
                break
            try:
                for i in range(0,len(ai_t)):
                    if ai_a[i]!=tb_a[i]:
                        diff+=1
                        print("\t"+str(j)+"\t"+tmap[ai_t[i]]+"\t"+amap[ai_a[i]]+"\t"+amap[tb_a[i]]+"\t"+pdic[j])
                        f.write(str(j)+"\t"+pdic[j]+"\t"+tmap[ai_t[i]]+"\t"+amap[ai_a[i]]+"\t"+amap[tb_a[i]]+"\n")
            except:
                print("LENGTH is not matched...stop evaluation.",len(ai_a),len(tb_a),"product id:",j,pdic[j])
                error+=1
                #break
        f.close()
    print(round((100-diff/total),3),"%, Errors: ",error)



'''
~~~~ 유저사전 영향 평가 ~~~~~~
(1) 사전 1과 사전 2 사이에 상이한 모든 토큰을 소환..

ARG
0. DB 패스워드
1. AS-IS 분석기 버전
2. TO-BE 분석기 버전
3. user사전 버전 --> (as-is와 to-be가 같아야 한다.)
4. 데이터 세트 

'''
def evaluationDic(db_password, asis, tobe, n_ver, dset):
    #카테고리 사전
    cdic, azdic, ddic, tdic, adic, pdic, udic = getParameterDics(db_password)
    # 1.연결
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    curs = conn.cursor()
    curs.execute("select date,uid from userdic")
    udic = dict(curs.fetchall())
    # 2.비교셑 모아오기(AS_IS)
    if (dset in ddic) and (asis in udic) and (tobe in udic):
        curs.execute("select * from result where did="+str(ddic[dset])+" and uid="+str(udic[asis])+" and ver="+str(azdic[n_ver]))
        ai = curs.fetchall()
        # 3.비교셑 모아오기(TO_BE)
        curs.execute("select * from result where did="+str(ddic[dset])+" and uid="+str(udic[tobe])+" and ver="+str(azdic[n_ver]))
        tb = curs.fetchall()
        total = len(tb)
    diff = 0
    print("\t토큰번호\t대상토큰\t이전버전분석\t비교버전분석")
    for i in range(0,len(ai)):
        #print(i)
        if ai[i][6]!=tb[i][6]:
            diff+=1
            print("\t",pdic[ai[i][1]])
            # DESCRIPTION: 
            print("\t"+str(ai[i][1])+"\t"+tdic[ai[i][4]]+"\t"+adic[ai[i][6]]+"\t"+adic[tb[i][6]])
    print(round((100-diff/total),3),"%")

    conn.close()

'''
Statistics
para
'''
def statistics(db_password,dset,cat,n_ver,u_ver):
    cdic, azdic, ddic, tdic, adic, pdic, udic = getParameterDics(db_password)
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    curs = conn.cursor()
    curs.execute("select * from result where ver="+str(azdic[n_ver])+" and did="+str(ddic[dset])+" and uid="+str(u_ver))
    res = curs.fetchall()
    #DATA : [ (0)rid (1)PID (2)TTID (3)CID (4)TID (5)DID (6)AID (7)VER (8)UID ]
    terms = {}
    for i in res:
        #print(tdic[i[4]],adic[i[6]])
        sp = adic[i[6]].split(" ")
        for s in sp:
            if s in terms:
                terms[s]+=1
            else:
                terms[s]=1
    sorted_terms = sorted(terms.items(), key=lambda kv: kv[1])
    print(sorted_terms)


'''
형태 분석기의 분석 결과를 분석하기 위한 방법.
# 특정 데이터 셋에 대한 특정한 버전의 분석기에서의 분석결과 모두를 가지고, 
'''
def getVariants(db_password, ver, dset):
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    curs = conn.cursor()
    cdic, azdic, ddic, tdic, adic, pdic, udic = getParameterDics(db_password)
    tcnt = 0
    acnt = 0
    print("LOAD TDIC...")
    curs.execute("select max(pid) from product where did="+str(dset))
    mx = curs.fetchall()
    curs.execute("select tid,ttid from tokens where pid<"+str(mx[0][0]))
    tz = dict(curs.fetchall())
    print(len(tz))
    # 토큰 사전에서 해당 버전에서 사용하는 모든 토큰을 소환
    for t in tz:
        # 형태소 분석 결과에서 분석형번호, 문장번호를 가져온다.
        curs.execute("select aid, pid from result where tid="+str(t)+" and did="+str(dset)+" and ver="+str(ver))
        tok = dict(curs.fetchall())
        if tok:
            tcnt+=1
            # variant가 있으면 결과 리스트는 1보다 클 것임.
            if len(tok)>1:
                acnt +=1
                for a in tok:
                    curs.execute("select * from result where aid="+str(a))
                    res = curs.fetchall()
                    print(tdic[t]+"\t"+adic[a]+"\t"+str(a)+"\t"+str(len(res))+"\t"+pdic[res[0][1]])
    print("총 토큰 타입: ",tcnt)
    print("총 변종토큰 타입: ",acnt)


    conn.close()

'''
형태 분석기의 분석 결과를 분석하기 위한 방법.
# 특정 데이터 셋에 대한 특정한 버전의 분석기에서의 분석결과 모두를 가지고 변종을 추출.
(1) {문장번호:텍스트} 사전
(2) {토큰번호:토큰}, {분석형번호:분석형} 사전
(3) 
'''
def variantTEST(db_password, dics, dset, n_ver, u_ver, tbag, worker):
    pdic = dics[6]
    #dset = 2  # P2
    #n_ver = 1 # N7.2_T8
    u_ver = 2 # 191028

    res = pd.read_csv("./db/result.csv")
    #cdic = pd.read_csv("./db/category.csv")

    #토큰 맵,  분석형 맵
    tmap = dics[4]
    amap = dics[5]

    # 전체 토큰 카운트
    tcnt = 0
    # 분석 토큰 카운트
    acnt = 0
    # 오류로 추정되는 분석 토큰 카운트
    error_candidates = 0
    #분석 폼들 중에 특정 버전, 특정 데이터 셋에 해당하는 것만 골라낸다. 
    rez = res.query('ver=='+str(n_ver))
    rez = rez.query('did=='+str(dset))
    rez = rez.query('uid=='+str(u_ver))
    #rez = rez.query('uid=='+str(udic[u_ver]))

    fw = open("./02.variantTEST/"+worker+".txt","w",encoding='utf-8')
    #문제적 토큰 번호만 고려해 본다.
    # t는?  --> 상품 데이터에 들어 있는 모든 토큰 타입...
    print(worker,":",len(tbag))
    for t in tbag:
        #print(len(tbag))
        #if len(tbag)%cnt==0:
        #    print(str((cnt/len(tbag))*100)+"%->",end="")
        #cnt+=1
        # "111" 번 토큰에 해당하는 모든 결과물 리스트 가져옴.
        toks = rez.query('tid=='+str(t))
        tcnt+=len(toks)
        toks = toks.to_dict()
        #print(toks)
        az = dict(toks['aid'])
        pz = dict(toks['pid'])
        #a_forms 구조: {1223:['1번문장','100번문장',...]}
        a_forms = {}
        for i in az:
            if az[i]==77:
                continue
            if az[i] in a_forms:
                a_forms[az[i]] = a_forms[az[i]] + [pz[i]]
            else:
                a_forms[az[i]] = [pz[i]]
        if len(a_forms)>1:
            #토탈 계산을 위한 임시 적은 값 
            minz = 10000
            for ii in a_forms:
                acnt += len(a_forms[ii])
                #print(tmap[t],amap[ii],len(a_forms[ii]),pdic[a_forms[ii][0]])
                try:
                    fw.write(pdic[a_forms[ii][0]]+"\t"+tmap[t]+"\t"+str(amap[ii])+"\t"+str(len(a_forms[ii]))+"\n")
                except:
                    print("ERROR float??:..",tmap[t],amap[ii],str(len(a_forms[ii])),pdic[a_forms[ii][0]])
                if minz > len(a_forms[ii]):
                    minz = len(a_forms[ii])
            error_candidates+=minz
    fw.close()
    print(" 전체 토큰 수:\t"+str(tcnt))
    print(" 전체 변종 토큰 수:\t"+str(acnt))
    print(" 전체 변종 에러 토큰 수:\t"+str(error_candidates))
    print(" 변종으로 인한 에러 비율:\t",round((round((error_candidates)/tcnt,8))*100,4),"%")
    return (tcnt,acnt,error_candidates)


        

'''
DB안의 일정 데이터 세트를 가져와
ES로 분석하여 문장당 단일토큰으로 json으로 저장한다.
'''
def getAnalyzed(db_password,did):
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    curs = conn.cursor()
    curs.execute("select pid,text from product where did="+str(did))
    senz = dict(curs.fetchall())
    res = {}
    for i in senz:
        sen = senz[i]
        ln = sen.rstrip("\r\n")
        ln = ln.replace('"','“')
        ln = ln.replace("'","`")
        #print(ln)
        try:
            tl = core.query_analysis("9201",CFG["es_index"],CFG["es_analyzer"], ln)
            ts, om = core.makeTokenstream(tl)
        except:
            print("ERROR:",ln)
            continue
        tz = ts['word']
        moa =""
        for a in tz:
            moa = moa+tz[a]+" "
        moa = moa.rstrip(" ")
        res[sen] = moa
    # JSON 포맷으로 저장.
    with open("./TSET_D1/keyword_analyzed.json","w",encoding='utf-8') as f:
        json.dump(res,f,ensure_ascii=False, indent=4)


'''
온당한 가정: 키워드와 분석 결과가 최대한 같을 수록 좋은 분석이다.
- 
- 
>
'''
def QI_TEST(db_password, port,did, ver, keywords, delimiter):
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    curs = conn.cursor()

    tz = pd.read_csv("./db/tokenMappings.csv")
    tmap2 = dataframe2dict(tz,"token","tid")

    tz = pd.read_csv("./db/analysis.csv")
    amap2 = dataframe2dict(tz,"res","aid")
    #결과
    res = pd.read_csv("./db/result.csv")
    rez = res.query('ver=='+str(ver))
    rez = rez.query('did=='+str(did))
    resultz = rez.query('uid=='+str(2))

    pz = pd.read_csv("./db/product.csv")
    pdic = dataframe2dict(pz,"pid","text")

    data = open("./TSET_K/keywords.txt","r")
    print("Data load..")
    keys = {}
    for k in data:
        k = k.rstrip("\r\n")
        k = k.split("\t")
        keys[k[0]] =k[1]
    #각종 사전들 모아오기.
    udic, cdic, ddic, vdic, tmap, amap = getDics()

    # 토큰 사전에서 모든 토큰을 소환

    dcnt=0

    match_t = 0
    exact_t = 0
    target_t = 0

    for k in keys:
        if dcnt<10000:
            dcnt+=1
            continue
        match=0 
        exact=0
        w = keys[k]
        # '에어팟' 이 들어있는 모든 상품명들.
        curs.execute("select pid from product where text like '%"+k+"%' and did="+str(did))
        record = curs.fetchall()
        #print(k,w,len(record))
        #토큰이 토큰 매핑에 존재해야함
        #print(k)
        tid = 0
        if k in tmap2:
            tid = tmap2[k]
        try:
            tl = core.query_analysis(port,CFG["es_index"],CFG["es_analyzer"], k)
            ts, om = core.makeTokenstream(tl)
            atoken = ""
            tz = ts['word']
            for s in tz:
                atoken += tz[s] + " "
            atoken = atoken.rstrip(" ")
            if atoken in amap2:
                aid = amap2[atoken]
            # 문장들...
            for p in record:
                pid = p[0]
                rez = resultz.query('pid=='+str(pid))
                rez = rez.query('tid=='+str(tid))
                #분석 결과에 단일 "에어팟"이 들어 있을 때,
                rez = rez.to_dict()['aid']
                if rez:
                    unmatch = True
                    #
                    for r in rez:
                        if rez[r]==aid:
                            exact+=1
                            unmatch=False
                            break
                    if unmatch:
                        print(k,amap[aid],pdic[p])
                    match+=1
            # core.getMapping(i, tokens[i], tmap, ts['word'], om, db_password)
            # (1) 주어진 문장들에서 완결된 해당 토큰이 있는지?  "에어팟"(O), "핏에어팟으로"(X)

            #print("키워드: ",k," 완전 매칭 토큰 수:",match," 총 들어있는 상품 수:",len(record),"분석 일치 개수: ",exact)
        except:
            print("ERROR:",k)
        match_t+=match
        target_t+=len(record)
        exact_t+=exact
        if dcnt==11000:
            break
        dcnt+=1
    print("완전 매칭 토큰 수:", match_t," 총 들어있는 상품 수:",target_t,"분석 일치 개수: ",exact_t)






def mergeRes(cat_names):        
    f1 = open("./res/test_res_total.txt","w")
    for ct in cat_names:
        f = open("./res/test_res_"+ct+".txt","r",encoding="utf-8")
        for x in f:
            f1.write(x)
        f.close()
    f1.close()


def makeOneKeywords():
    keys = {}
    with open("./TSET_D1/inner.txt","r") as d1:
        for l in d1:
            l = l.rstrip("\r\n")
            l = l.strip(" ")
            l = l.replace('"','“')
            l = l.replace("'","`")
            kz = l.split(" ")
            for k in kz:
                if not k in keys:
                    keys[k] = 0
    with open("./TSET_D1/innerKeywords.txt","w") as d2:
        for k in keys:
            d2.write(k+"\r\n")
    
    print(keys)
    print(len(keys))


'''
타겟 데이터의 빈도수 조사 
'''
def statsTEST(cat):
    title = {}
    title_p = {}
    total = {}
    scnt = 0
    for c in cat:
        with open("./TSET_D1/"+c+".txt","r") as d1:
            for l in d1:
                l = l.rstrip("\r\n")
                #문장번호 증가
                scnt+=1
                if len(l)>0:
                    try:
                        #tl = query_analysis("9201",CFG["es_index"],CFG["es_analyzer"],l.replace("'","`").replace('"','\"'))
                        tl = core.query_analysis("9201",CFG["es_index"],CFG["es_analyzer"],l.replace("'","`").replace('"','“'))
                    except:
                        print("ERROR:  "+l+"  remove..this line..")
                        continue
                    try:
                        ts, om = core.makeTokenstream(tl)
                        wdz = ts['word']
                        for i in wdz:
                            wd = wdz[i]
                            #문장번호 하나씩 추가
                            if not wd in title_p:
                                title_p[wd] = {scnt:0}
                            else:
                                aa = title_p[wd]
                                aa[scnt] = 0
                                title_p[wd] = aa
                            #
                            if wd in title:
                                title[wd]+=1
                            else:
                                title[wd]=1
                            if wd in total:
                                total[wd]+=1
                            else:
                                total[wd]=1
                    except:
                        print("ERROR: " +l)
                        continue
        # JSON 포맷으로 저장.
        print("WRITE ",c)
        with open("./TSET_D1/"+c+"_out.txt","w",encoding='utf-8') as f:
            for i in title:
                f.write(i+"\t"+str(title[i])+"\t"+str(len(title_p[i]))+"\r\n")
        title = {}
        title_p = {}
    print("WRITE total")
    with open("./TSET_D1/target_total.txt","w",encoding='utf-8') as f:
        for i in total:
            f.write(i+"\t"+str(total[i])+"\r\n")


'''

'''
def saveDB2CSV(db_password):
    tables = ["userdic","tokenMappings","product","category","dataset","tokens","analyzer","analysis","result"]
    conn = pymysql.connect(host="localhost",user='root',password=db_password, db='anal_eval',charset='utf8')
    curs = conn.cursor()
    print("START")
    for t in tables:
        print("write "+t+".csv...")
        curs.execute("show columns from "+t)
        columns = [a[0] for a in curs.fetchall() ]
        curs.execute("select * from "+t)
        rows = curs.fetchall()
        rows = list(rows)
        for a in range(len(rows)):
            rows[a] = list(rows[a])
        with open("./db/"+t+'.csv', 'w', encoding='utf-8', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(columns)
            for i in range(len(rows)):
                wr.writerow(rows[i])
        f.close()
    conn.close()
