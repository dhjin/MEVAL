import re
import csv

CONFIG={
    #상품세트 1
    "P1":"./TSET_P1",
    #상품세트 1에 관한 정답세트 1
    "P1G1":"./TSET_P1/GS01/",
    "P2":"./TSET_P2",
    "P2G1":"./TSET_P2/GS01/"
}


'''
두 표현에 따른 모든 섭스트링 모아치기...
(1) 파일을 받는다. 파일 형식 --> [0]카테고리--[1]알파뉴메릭 여부--[2]원문--[3]분석1전체--[4]분석1토큰--[5]--정합성필드--[6]분석2전체--[7]분석2토큰
(2) 다음의 사전을 돌려준다. 
    {"가나다":[(가,가나),(나,),(다,다)]}
'''
def getMatches(f):
    en = {}
    idx = {}
    #토큰 정보에 넣을 카운트 수
    cnt = 0
    original_text = ""
    for i in f:
        i = i.rstrip("\r\n")
        iz = i.split("\t")
        s = iz[3]
        #인덱스 사전에서 새로운 텍스트 등장시
        if not original_text==iz[1]:
            cnt+=1
            idx[cnt] = (iz[0],iz[1])
        if iz[1] in en:
            lst = en[iz[1]]
            lst.append(s)
            en[iz[1]] =  lst
            
        else:
            en[iz[1]] = [s]
        
        original_text = iz[1]
    return en, idx


'''
(1)과정에서 모은 원본의 섭스트링 리스트들을 통해
    - original_string: "[TV]파세코 창문형에어컨+파세코 모스클린"
    - analyzed_tkn_list: [...('형 에어컨','형에 어컨)  ...]
    - ord: 0 or 1
'''
def b(original_string,analyzed_tkn_list):
    #라인 받기.
    pos = 0
    a_token_set = []
    ret = {} # diff결과
    #print(a1)
    # 오리지날 문장을 공백 기준으로 가른 원본 토큰들.
    # "[TV]파세코 창문형에어컨+파세코 모스클린" ==> ['[TV]파세코','창문형에어컨+파세코','모스클린']
    ln  = re.split(' ',original_string) 
    #분석된 토큰 리스트 순환하기 위한 인덱스
    chk = False
    #if "7/1~3] 박싱데이" in original_string:
    #    print(original_string,analyzed_tkn_list)
    #    chk = True
    i = 0
    while pos<len(ln): 
        # 오리지널 토큰들을 돌기
        o_token = ln[pos]
        # o_token = '[TV]파세코' ==> '[tv]파세코'
        o_token = o_token.lower()
        # MD's ==> mds
        o_token = o_token.replace("'","")
        if o_token:
            while i < len(analyzed_tkn_list):
                a_token = analyzed_tkn_list[i]
                
                #a_token = a_tokens[ord]
                if chk:
                    print(o_token,a_token)
                
                if " " in a_token:
                    split_a_token = a_token.split(" ")
                    #중간이 공백이 들어간 토큰이 발견 되면 일단 이전에 쌓아오던 토큰은 여기서 종결을 본다.
                    ############################
                    if not split_a_token[0] in o_token:
                        ret[o_token] = " ".join(a_token_set)
                        a_token_set = []
                        pos+=1
                        o_token = ln[pos]
                        o_token = o_token.lower()
                    ############################
                    for z in range(0,len(split_a_token)):
                        ztkn = split_a_token[z]
                        if ztkn in o_token:
                            #print("ztkn>>>",a_token_set)
                            a_token_set.append(ztkn)
                            ## "+ 모기" 같이 분할된 것이 원래 토큰 "+"에 합치할 때, "모기"를 원래 자리에 집어 넣기
                            if o_token=="".join(a_token_set) :
                                if chk:
                                    print(o_token,"".join(a_token_set),"---"," ".join(split_a_token[z+1:]))

                                analyzed_tkn_list[i] = ("x"," ".join(split_a_token[z+1:]))
                                break
                    #싱글+서랍도어옷장 ==> "싱글", "+ 서랍", "도어", "옷장"
                    if not o_token=="".join(a_token_set):
                        #print("-->",o_token,"".join(a_token_set))
                        i+=1
                        continue
                ############################
                if a_token in o_token:
                    a_token_set.append(a_token)
                    i+=1
                    if chk:
                        print("INCREMENT: ",a_token_set)
                    # 원래 토큰과 분석토큰 모두 공백을 없앤것이 서로 같다면? 결과 사전에 집어 넣기.
                    # 예> a_token.strip==>'창문형에어컨+파세코', o_token.strip ==>'창문형에어컨+파세코'
                    if o_token=="".join(a_token_set):
                        break 
                else:
                    break
            ret[o_token] = " ".join(a_token_set)
            a_token_set = []
        pos+=1
    return ret

'''
입력:
    alz: {..."페르시안 카페트":[(페르시안, 페르시안),(카페트,카페트)] ...}
    id_dic: { 0:('가구디지털','X','페르시안 카페트')}
(2) 다음의 사전을 도출해낸다. 정수 0~n 으로 된 인덱스를 (문장번호,토큰번호) 식으로 변경한 iddic 얻어낸다.
    두개의 ES서버로부터 얻은 diff 내용(alz)으로부터 
    iddic: {(1,1):("식품생활","X","부르고뉴")} // 토큰 프로파일
    alles: {(1,1):("부르고 뉴","부르고뉴") } // diff 결과
    gs_dic: {(1,1):("부르고뉴")} // 정답사전
'''
def getDicts(alz,id_dic):
    cnt = 0
    alles = {}
    gs_dic = {} # 정답사전
    for a in id_dic:
        text = id_dic[a][1]
        en = alz[text]
        test = b(text,en)
        cnt = a
        ccnt=0
        for r in test:
            ccnt+=1
            #print(str((cnt,ccnt))+"\t"+a+"\t"+r+"\t"+test[r])
            alles[(cnt,ccnt)] = (r,test[r])
            gs_dic[(cnt,ccnt)] = (r,test[r])
    return alles, gs_dic


'''
dict 형식의 데이터를 라인구분의 파일로 바꾼다. 
'''
def toFile(path, dic):
    f = open(path,"w")
    for k in dic:
        f.write(k+"\t"+dic[k]+"\n")

'''
"Product 2" 데이터세트의 "Guidline 1" 정답 파일을 읽어온다.

'''
def getTestSET(path):
    test_path = CONFIG[path]
    testset = {}
    with open(test_path+'correct.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            testset[row[0]] = row[2]
    return testset


