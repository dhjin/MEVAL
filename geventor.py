import gevent
from gevent import monkey
from gevent import Greenlet
import autotest
import pymysql
import pandas as pd
import multiprocessing
import autotest as at
import time
from multiprocessing import Process
from multiprocessing import Pool

data = list(range(0,80000))

def plus(dat):
    total = 0
    for i in dat[0]:
        total+=i
    print(dat[1],len(dat[0]),total)


### 
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
def compute(data,func):
    monkey.patch_all()
    total_workers = ['1-worker', '2-worker', '3-worker', '4-worker', '5-worker', 
                    '6-worker', '7-worker', '8-worker', '9-worker', '10-worker',
                    '11-worker', '12-worker', '13-worker', '14-worker', '15-worker']

    # 4677715
    # step size는 worker 개수를 기준으로 계산한다.
    step = 4000

    jobs = []
    for i, each_worker_name in enumerate(total_workers):
        if i == len(total_workers) - 1:
            jobs.append(gevent.spawn(func, [data[step*(i):], each_worker_name]))
        else:
            jobs.append(gevent.spawn(func, [data[step*(i):step*(i+1)], each_worker_name]))
    ###
    gevent.joinall(jobs)

### 
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
def computeVariant(db_password, dset, n_ver, u_ver, func):
    #상품데이터. max 번호, min 번호 설정.
    start = time.time()
    p = pd.read_csv("./db/product.csv")
    pdic = at.dataframe2dict(p,"pid","text")
    pdct = p.query('did=='+str(dset))
    
    mn = min(pdct['pid'][:])
    #mx = max(pdct['pid'][:])
    mn += 10000
    mx = mn+10000
    #mx = 248871
    print(mn,"~",mx)

    total_workers = ['w-1','w-2','w-3','w-4','w-5','w-6','w-7','w-8']
    # 4677715
    # step size는 worker 개수를 기준으로 계산한다.
    tdic = pd.read_csv("./db/tokens.csv")
    target = tdic.query('pid<'+str(mx+1)+' and pid>'+str(mn-1))
    #특정 데이터셋에 속하는 문장을 고르고, 거기에 해당되는 토큰 번호를 솎아낸다.
    tz = target['tid']
    #bag: 특정 토큰 타입.
    bag = {}
    for t in tz:
        bag[t] = 0
    tokens = list(bag.keys())
    step = int(len(tokens)/len(total_workers))
    print(step)
    monkey.patch_all()

    jobs = []
    res = []


    udic, cdic, ddic, vdic, tmap, amap = at.getDics()
    dics = (udic, cdic, ddic, vdic, tmap, amap, pdic)

    for i, each_worker_name in enumerate(total_workers):
        if i == len(total_workers) - 1:
            #DB에서 데이터 가져옴
            jobs.append(Greenlet.spawn(func, "Cnupo29vn!", dics, dset, n_ver, u_ver, tokens[(step*i):len(tokens)], each_worker_name))
            #p = Process(target=func,args=("Cnupo29vn!", dics, dset, n_ver, u_ver, tokens[(step*i):len(tokens)], each_worker_name))
            #p.start()
            #p.join()
        else:
            jobs.append(Greenlet.spawn(func, "Cnupo29vn!", dics, dset, n_ver, u_ver, tokens[(step*i):(step*(1+i))], each_worker_name))
            #p = Process(target=func,args=("Cnupo29vn!", dics, dset, n_ver, u_ver, tokens[(step*i):(step*(1+i))], each_worker_name))
            #p.start()
            #p.join()

    ###
    gevent.joinall(jobs)
    
    wf = open("./02.variantTEST/test_result_"+str(dset)+"_"+str(n_ver)+"_"+str(u_ver),"w")
    for w in total_workers:
        with open("./02.variantTEST/"+w+".txt","r") as f:
            for l in f:
                wf.write(l)
    wf.close()
    
    tcnt = 0 # 총 토큰 개수
    acnt = 0 # 총 변종 분석 개수
    ecnt = 0 # 오분석으로 보이는 변종 개수
    for r in jobs:
        tcnt += r.value[0]
        acnt += r.value[1]
        ecnt += r.value[2]
    print("-----------------------------")
    print("전체 토큰 수:\t"+str(tcnt))
    print("전체 변종 토큰 수:\t"+str(acnt))
    print("전체 변종 에러 토큰 수:\t"+str(ecnt))
    print("변종으로 인한 에러 비율:\t",round((round((ecnt)/tcnt,8))*100,4),"%")
    print("time :", time.time() - start)






    
    