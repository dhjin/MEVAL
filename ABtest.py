from elasticsearch import Elasticsearch
from elasticsearch import client
import pandas as pd
import difflib


'''
IN: es port number
OUT: ES index
'''
def getES(port):
    es = Elasticsearch("localhost:"+port+"/")
    index = client.IndicesClient(es)
    return index


def getTS(B, dset, index_cfg, cut, verbose):
    with open("T10_TS.txt","w") as f:
        f.write("입력\tORIGIN\tWORD\tSYNONYM\n")
        for i in range(len(dset)):
            sen = dset[i]
            body = {"analyzer":"nori_custom",
                "text":sen,
                "explain":"false"}
            b = B.analyze(index_cfg,body)
            ts_b = b['tokens']
            orig = []
            syn = []
            word = []
            for t in ts_b:
                if t['type'] == 'ORIGIN':
                    orig.append(t['token'])#, t['start_offset'],t['end_offset'],t['position']))
                elif t['type']=='SYNONYM':
                    syn.append(t['token'])#, t['start_offset'],t['end_offset'],t['position']))
                else:
                    word.append(t['token'])
            f.write(sen+"\t"+",".join(orig)+"\t"+",".join(word)+"\t"+",".join(syn)+"\n")
            if i==cut:
                break


'''
- A: 비교 대상 ES 인덱스
- B: 개선된 ES 인덱스
- dset: 비교에 쓸 데이터셋, (스트링의 리스트)
- index_cfg: 대상 데이터 테이블명
- cut: 몇개까지 볼것인가
- verbose: 메시지 출력 여부

결과물로 두개의 파일을 생성. 
OUT: 통계 데이터 사전
'''
def ABtest(A, B, dset, index_cfg, cut, verbose):
    errors = open("ERRORS.txt","w")
    result = open("RESULT.txt","w")
    cnt=0
    error_cnt = 0
    outs = {} #최종 결과물
    tok_cnt = 0
    for i in range(len(dset)):
        sen = dset[i]
        body = {"analyzer":"nori_custom",
            "text":sen,
            "explain":"false"}
        a = A.analyze(index_cfg,body)
        b = B.analyze(index_cfg,body)
        ts_a = a['tokens']
        ts_b = b['tokens']
        # om? --> 토큰스트링 --오프셋 매핑: 예> { 0:(0,3)}  -- "강아지"
        
        try:
            a_ts, a_om = makeTokenstream(ts_a)
            b_ts, b_om = makeTokenstream(ts_b)
            tok_cnt+=len(a_om)
        except:
            #print(i,"ERROR",sen)
            #print(ts_a)
            #print(ts_b)
            errors.write(str(ts_a)+"\t"+str(ts_b)+"\n")
            error_cnt+=1
            continue

        #--- 리스트에 집어넣기
        mx = max(len( a_ts['word']),len(b_ts['word']))
        alst = []
        blst = []
        for j in range(mx):
            if j in a_ts['word']:
                alst.append(a_ts['word'][j])
            if j in b_ts['word']:
                blst.append(b_ts['word'][j])
        #------------------
        if not alst==blst:
            #print("SENTENCE: ",i,sen)
            #print("A:\t"+" ".join(alst))
            #print("B:\t"+" ".join(blst))
            diff_res = difflib.ndiff(alst,blst)
            rrs = findOffsets(sen, diff_res,b_om, alst, a_om)
            try:
                (orig, a_string, b_string) = rrs
                #print(orig, s, e, a_string, b_string)
                result.write(sen+"\t"+" ".join(alst)+"\t"+" ".join(blst)+"\t"+orig+"\t"+a_string+"\t"+b_string+"\n")
                if rrs in outs:
                    outs[rrs] +=1
                else:
                    outs[rrs] = 1
            except:
                #print(i,"ERROR",sen)
                #print(ts_a)
                #print(ts_b)
                errors.write(str(ts_a)+"\t"+str(ts_b)+"\n")
                error_cnt+=1
                continue
            cnt+=1

        if i%10000==0 and i!=0 and verbose==True:
            print("오류: ",cnt,"\t대상상품수: ",i,"\t비율: ",round((cnt/i)*100,4),"\t파싱 오류: ",error_cnt)
        if i==cut:
            break
    '''

    '''
    result.close()
    errors.close()
    out_sort = sorted(outs.items(),key=f1,reverse=True)
    stat = open("STATS.txt","w")
    for s in out_sort:
        stat.write(str(s[0][0])+"\t"+str(s[0][1])+"\t"+str(s[0][2])+"\t"+str(s[1])+"\n")
    stat.close()
    print("전체 대상 토큰 수: ",tok_cnt)
    return outs


def findOffsets(sen, ts, om, ts_compare,om_compare):
    c = 0
    orig = ""
    b_st = ""
    ts_lst = list(ts)
    #- 표시된 것이 몇 개인지 카운트 ts의 정확한 위치에서 분석 토큰을 가져오기 위함
    minus_count = 0
    b_st_candidate = ""
    #print(ts_lst)
    #print(om)
    for t in ts_lst:
        if t.startswith("?"):
            minus_count +=1
            continue
        if t.startswith("- "):
            b_append = t[1:].strip(" ")
            b_st_candidate+=b_append
            #print(b_st_candidate)
            minus_count +=1
            continue
        if c in om:
            if t.startswith("+ "):
                s = om[c][0]
                e = om[c][1]
                b_st = t[1:].strip(" ")
                #find prefix
                forward = c-1
                while forward in om:
                    f_token = om[forward]
                    #앞 토큰의 시작 오프셋, 끝 오프셋
                    f_s = f_token[0]
                    f_e = f_token[1]
                    if s==f_e: #앞토큰이랑 연결됨.(단일토큰이라는말)
                        s = f_s
                        f_token_st = ts_lst[forward].strip(" ")
                        if f_token_st.startswith("+ "):
                            f_token_st = f_token_st[2:]
                        #만약 + 표시가 있으면 떼준다.
                        b_st = f_token_st +" "+b_st
                    else:
                        break
                    forward-=1
                #find suffix --diff 결과를 순차적으로  읽는데, - 표시는 뛰어 넘어야 함..
                backward = c+1
                while backward in om:
                    b_token = om[backward]
                    #print("backward--",backward)
                    #뒤 토큰의 시작 오프셋, 끝 오프셋
                    b_s = b_token[0]
                    b_e = b_token[1]
                    if e==b_s: #뒤 토큰이랑 연결됨.(단일토큰이라는말)
                        
                        try:
                            b_token_st = ts_lst[minus_count+backward].strip(" ")
                        except:
                            print("EXCEPTION: ",b_token,"\t",om,"END_TOKEN: ",e)
                            return (orig ,b_token, b_st)
                        # 중간에 등장하는 - 토큰에 대한 처리..
                        if b_token_st.startswith("- "):
                            b_append = t[1:].strip(" ")
                            b_st_candidate+=b_append
                            minus_count +=1
                            continue
                        if b_token_st.startswith("?"):
                            minus_count +=1
                            continue
                        e = b_e
                        if b_token_st.startswith("+ "):
                            b_token_st = b_token_st[2:]
                        b_st = b_st+" "+b_token_st
                        #print(">>",b_st)
                    else:
                        break
                    backward+=1
                orig = sen[s:e]
                a_st = ""
                # find correspond string in Comparing OMs..
                #
                try:
                    for x in om_compare:
                        cm = om_compare[x]
                        if cm[0] >=s and cm[1] <=e:
                            a_st += " "
                            a_st += ts_compare[x]
                    a_st = a_st.lstrip(" ")
                except:
                    print("ERROR: ",b_st_candidate,":::",a_st)
                    a_st = b_st_candidate
                return (orig, a_st, b_st)
        c+=1
    return None

def f1(x):
    return(x[1])

def makeTokenstream(tl):
    #포지션-오프셋 맵핑
    #예> 토큰: 알리바바 ==> 0 -- (0,4)
    offset_mapping = {}
    tokenstream = {"word":{},"synonym":{}}
    #tl = tl['tokens']
    for t in tl:
        tp = t['type']
        poz = t['position']
        
        if tp=="word":
            words = tokenstream["word"]
            words[poz] = t["token"]
            tokenstream["word"]=words
            #나중에 토큰-분석형 매핑할 때 사용한다.
            offset_mapping[poz]=(t["start_offset"],t["end_offset"])
        elif tp=="SYNONYM":
            synz = tokenstream["synonym"]
            if not synz:
                synz[poz] = t["token"]
            elif not poz in synz:
                synz[poz] = t["token"]   
            else: # 이미 동의어가 들어차 있을때,
                c = 1
                while(True):
                    #토큰스트림 안이 "synonym X"라는 행이 있을 때.. 
                    if "synonym"+str(c) in tokenstream:
                        syn_b = tokenstream["synonym"+str(c)]
                        if poz in syn_b:
                            c+=1
                            continue
                        else:
                            syn_b[poz] = t["token"]
                            tokenstream["synonym"+str(c)] = syn_b
                            break
                    else:
                        syn_b = {poz:t["token"]}
                        tokenstream["synonym"+str(c)] = syn_b
                        break
                continue
            tokenstream["synonym"] = synz
    if not tl:
        return {}
    #print(html)
    return tokenstream, offset_mapping

'''
Arirang-노리 비교 파일까리.
파일 포맷은...
1. 원본파일: "분석한다"
2. A파일:[[분석,한다], ...[..]]
3. B파일:[[분석,한다], ...[..]]
'''
def ABtestFile(A, B, dset, cut, verbose):
    errors = open("ERRORS.txt","w")
    result = open("RESULT.txt","w")
    cnt=0
    error_cnt = 0
    outs = {} #최종 결과물
    for i in range(len(dset)):
        sen = dset[i]
        ts_a = A[i].lower()
        ts_b = B[i]
        alst, a_om = parseNori(ts_a)
        blst, b_om = parseArirang(ts_b)

        #--- 리스트에 집어넣기
 
        #------------------
        if not alst==blst:
            #print("SENTENCE: ", i, sen)
            #print("A:\t"+" ".join(alst))
            #print("B:\t"+" ".join(blst))
            diff_res = difflib.ndiff(alst,blst)
            #print(diff_res)
            rrs = findOffsets(sen, diff_res, b_om, alst, a_om)
            try:
                (orig, a_string, b_string) = rrs
                #print(orig, s, e, a_string, b_string)
                result.write(sen+"\t"+" ".join(alst)+"\t"+" ".join(blst)+"\t"+orig+"\t"+a_string+"\t"+b_string+"\n")
                if rrs in outs:
                    outs[rrs] +=1
                else:
                    outs[rrs] = 1
            except:
                #print(i,"ERROR",sen)
                #print(ts_a)
                #print(ts_b)
                errors.write(str(ts_a)+"\t"+str(ts_b)+"\n")
                error_cnt+=1
                continue
            cnt+=1

        if i%10000==0 and i!=0 and verbose==True:
            print("오류: ",cnt,"\t대상상품수: ",i,"\t비율: ",round((cnt/i)*100,4),"\t파싱 오류: ",error_cnt)
        if i==cut:
            break
    '''

    '''
    result.close()
    errors.close()
    out_sort = sorted(outs.items(),key=f1,reverse=True)
    stat = open("STATS.txt","w")
    for s in out_sort:
        stat.write(str(s[0][0])+"\t"+str(s[0][1])+"\t"+str(s[0][2])+"\t"+str(s[1])+"\n")
    stat.close()
    return outs


'''
Parse Nori..
노리의 분석결과: 더블에스몰/0,5 더블 에 스몰/0,5 플라워사각코스메틱백/6,16 플라워 사각 코스메틱 백/6,16 화장품가방/17,22 화장품 가방/17,22 여행가방/23,27 여행 가방/23,27 
원형 나오고, 오프셋 나옴.
'''
def parseNori(anal):
    orig = {}
    om = {}
    tokenList = []
    anal = anal.lower()
    tkns = anal.split("\t")
    inc = 0
    for t in tkns:
        if not t:
            continue
        ts = t.split("/")   # 더블에스몰 / 0,5
        offsets = ts[1]
        if offsets in orig: # 원형이 이미 들어가 있음 {"0,5":더블에스몰}
            origToken = orig[offsets]
            s_e = offsets.split(",")
            subs = ts[0].split(" ")
            s = int(s_e[0]) # 전체 시작토큰
            e = int(s_e[1]) # 전체 끝토큰
            cur_s = s
            if len(subs)==1:
                om[inc] = (s,e)
                tokenList.append(ts[0])
                cur_s = s
                cur_e = e
                inc+=1
                sub = []
                continue

            for i in range(0,len(subs)):
                tk = subs[i]
                tokenList.append(tk)
                #print(cur_s,s)
                cur_s = s + origToken.find(tk,(cur_s-s)) # 현토큰 시작 오프셋.. 현 섭토큰의 절대 위치 정보에서 오리지날 토큰의 절대 위치 정보를 빼면 복합명사의 길이에 맞는 위치 나옴.
                cur_e = cur_s + len(tk) # 현재 끝 오프셋
                #print(origToken,tk,inc,":(",cur_s,",",cur_e,")")
                om[inc] = (cur_s,cur_e)
                inc+=1
        else:
            orig[offsets] =ts[0]
    #print(tokenList,om)
    return tokenList,om

'''
더블에스몰/0,5 더블/0,2 에/2,3 스몰/3,5 플라워사각코스메틱백/6,16 플라워/6,9 사각/9,11 코/11,12 스메틱/12,15 백/15,16 화장품가방/17,22 화장품/17,20 가방/20,22 여행가방/23,27
'''
def parseArirang(anal):
    frontier = 0
    om = {}
    tokenList = []
    tkns = anal.split(" ")
    #print(tkns)
    inc = 0
    subs = ["!"]
    cand_tk = ""
    cand_se = None 
    #print(anal)
    for t in tkns:
        ts = t.split("/")
        offsets = ts[1].split(",")
        s = int(offsets[0])
        e = int(offsets[1])
        # 처음 나온 끝점보다 작은 끝점이면..
        if e <= frontier:
            tokenList.append(ts[0])
            
            # 아리랑에 존재하는 버그땜에 추가의 처리를 해야 한다.
            # 이전 첫점과 지금 첫점이 동일하다...
            if subs and om[inc-1][0]==s:
                e = om[inc-1][1] + (e-s)
                s = om[inc-1][1]
                
            om[inc] = (s,e)
            inc+=1
            subs.append(ts[0])
        #원본토큰이라 생각됨.
        else:
            frontier = e
            if not subs:
                tokenList.append(cand_tk)
                om[inc] = cand_se
                inc+=1
            cand_tk = ts[0]
            cand_se = (s,e)
            subs = []
    #마지막에 들어온 복합명사 원형이.. 최종으로 들어간 섭토큰의 오프셋보다 클때.
    if cand_se and (not om or cand_se[1] > om[inc-1][1]):
        tokenList.append(cand_tk)
        om[inc] = cand_se

    #print(tokenList,om)
    return tokenList, om
