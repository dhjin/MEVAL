import subprocess
import json

#===============================
#
#
#
#
#
#
#
#===============================
'''
DESC: 
(1) pn: 포트 넘버
(2) query: 서치 쿼리
(3) elastic_index: 엘라스틱 서치에 넣는 인덱스 이름
(4) elastic_analyzer: 분석기 이름
'''
def query_analysis(port,elastic_index,elastic_analyzer,query):
    cmd = """curl -X GET "http://localhost:%s/%s/_analyze" -H 'Content-Type: application/json' -d' { "analyzer": "%s", "text": ["%s"] }' """ % (port,elastic_index,elastic_analyzer,query)

    ps = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = ps.communicate()[0]
    result = output.decode('utf-8').splitlines()  # 라인 갈라서 결과물 얻어내기

    token_idx = ''.join(result).find('{"tokens"') 
    result = ''.join(result)[token_idx:]
    json_result = json.loads(result)
    #print(json_result)
    token_list = json_result['tokens']    
    return token_list



'''
ES 머신으로부터 받은 tokenstream json 분석,
- tokenstream 에는 word 목록 synonym 목록을 받고,
- offset_mapping에는 각 낱말 당 포지션:(시작 오프셋, 끝 오프셋) 정보를 받는다.
'''
def makeTokenstream(tl):
    #포지션-오프셋 맵핑
    #예> 토큰: 알리바바 ==> 0 -- (0,4)
    offset_mapping = {}
    tokenstream = {"word":{},"synonym":{}}
    #print(tl)
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
9. 원형 토큰과 분석토큰과의 매핑을 찾아낸다.
|idx:  
|dd:
'''
def getMapping(idx, tokens, tokenMap, dd, offset_mapping, db_password):
    #원본 토큰의 마지막 오프셋 위치 지정.
    start_pointer = 0
    end_pointer = 0

    # 순서: (tid,시작점,끝점) -- 사전에 집어넣기.
    #print(self.res[idx])
    result = {}
    #print(offset_mapping)
    for i in range(0,len(tokens)):
        # 현재의 원형 토큰,,
        t = tokens[i]
        start_pointer = t[4]
        end_pointer = t[5]
        j = 0
        analyzed = ''
        mx = 0
        for x in dd:
            if mx<x:
                mx = x
        #print(start_pointer,end_pointer)
        
        if t:
            while j < mx+1:
                if j in offset_mapping:
                    (s,e) = offset_mapping[j]
                    #print(j,":",s,e)
                    # j번째 분석 토큰의 offset이 현재 토큰의 범위 안에 있을 때..
                    if s>=start_pointer and e<=end_pointer:
                        analyzed+=dd[j]+" "
                j+=1
            #print(i,tokenMap[t[3]],analyzed)

        #분석 결과가 있을 때..
        result[i]= analyzed.rstrip(" ")
        end_pointer+=1
    return result