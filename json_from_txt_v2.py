# !/usr/bin/env python
# -*- coding=utf-8 -*-
import codecs
import os
import chardet
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


'''
task_id role speak_time message
read from txt
group data
write json
'''

 
 
# 获取文件编码类型
def get_encoding(file):
    # 二进制方式读取，获取字节数据，检测类型
    with open(file, 'rb') as f:
        data = f.read()
        return chardet.detect(data)['encoding']

def read_file(path,torder):
    #读取txt中的对话信息,txt为excel另存为已制表符分隔的txt文本
    if get_encoding(path) =="GBK" or get_encoding(path)=="GB2312":
        file = codecs.open(path, 'r', encoding="gbk")
    else:
        file = codecs.open(path, 'r', encoding="utf-8")
    lines = [line.strip().replace('\n', '') for line in file]
    line_list = []
    i= 0 ### 用于进度条
    data_len = len(lines)
    torder_list = torder.split(",")
    for line in lines:
        i=i+1
        print "读取进度: %d" % int(float(i)*100/float(data_len)), "▓" * int((float(i)*50/float(data_len))),"\r", 
        sys.stdout.flush()###用于进度条展示

        # if i==0:
            # i=i+1
            # continue
        line_dict = {}
        linedata = line.split("\t")
        if len(linedata) < 4:#空行,跳过
            continue
        line_dict["task_id"] = linedata[int(torder_list[0])-1]
        line_dict["role"] = linedata[int(torder_list[1])-1]
        line_dict["speak_time"] = linedata[int(torder_list[2])-1]
        line_dict["message"] = linedata[int(torder_list[3])-1]
        line_list.append(line_dict)
    print "" ###用于进度条展示
    return line_list


def group_data(datas, KEY='task_id'): #datas 所有处理成dict的数据 [{},{},{}]
    '''
    group by chat_session_id
    '''
    group_dic = {}
    group_array = []
    for data in datas:
        flag = False
        for dic in group_array:
            if dic['key'] == data[KEY]:
                flag = True
                dic['val'].append(data)
        if flag == False:
            dic = {'key':data[KEY], 'val':[data]}
            group_array.append(dic)        
    for i in group_array:
        #tdata = fun_re(i['val'])#倒序排列聊天语句,原语序反序时放开注释
        #group_dic[i['key']] = tdata
        group_dic[i['key']] = i['val']
    return group_dic

def fun_re(a):
    #倒排语句的处理方法,用于提供的原始文本时间顺序反续时调整语序
    b = []
    max = len(a)
    for i in xrange(max):
        b.append(a[max - 1 - i])
    return b 

def intergrate(data):
    '''
    in :分组后的字典  data = {'9771':M,'9762':L}
    out:合并相同数据后的字典
    '''
    unsame_key = ['speak_time', 'message', 'role']#若需要静音间隔,需再加间隔字段
    #KEY = []
    for k, v in data.items():#取每一个session的通话,在v中 v为list
        #after_inter 每个session压成一行
        after_inter = {}
        after_inter['CONTENT'] = []
        ##after_inter['PERIOD'] = 0
        #用第一句话,将同一session的相同字段先取到
        for ikey in v[0].keys():
            if ikey not in unsame_key:#相同字段取第一句话的值
                after_inter[ikey] = v[0][ikey]
        #for iv in v:#遍历每一句话
        for iv in xrange(len(v)):#遍历每一句话
            #after_inter['PERIOD'] = 0
            #划分角色
            #role= v[iv]['role']
            message = str(v[iv]['message'])
            after_inter['CONTENT'].append({'ROLENAME':v[iv]['role'], 'MESSAGE_TEXT':message, \
            'COMM_TIME':v[iv]['speak_time']})
            #after_inter['KEFUID'] = "客服"
        data[k] = after_inter
    return data

def str_datetime(v):
    return datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')

def write_json(data, base_path):
    json_name = '.'.join((str(data[0]), 'json'))#用0取tuple的第一个值
    convert_path = os.path.join(base_path, "json" , json_name)
    convert_text_main(data[1]['CONTENT'], convert_path)

def make_datasource(datas, base_path, dim_date,robot_id,agent_id):#创建数据源的目录
    #写json,dim文件
    #
    import time
    print ""  
    print ""  
    i = 0
    data_len = len(datas.items()) ### 用于进度条展示
    main_dim = ["任务流水号|文本列表|对话id"]
    sub_dim = ["文本流水号|FilePath|机器人客服ID|客服ID"]
    main_path = os.path.join(base_path, "text", "Main", dim_date)
    sub_path = os.path.join(base_path, "text", "Sub", dim_date)
    for data in datas.items():
        write_json(data, sub_path)
        main_dim.append(str(i) + "|" + str(i)+"|"+str(data[0]))
        sub_dim.append(str(i) + "|json/" + (str(data[0]) + ".json|"+robot_id+"|"+agent_id))
        i = i + 1
        print "写入进度: %d" % int(float(i)*100/float(data_len)), "▓" * int((float(i)*50/float(data_len))),"\r", 
        sys.stdout.flush()###用于进度条展示
        time.sleep(0.05)
    print "" ###用于进度条展示
    flagList = ['flag.new']
    writeFileByList(main_dim, os.path.join(main_path, dim_date + ".dim"))
    writeFileByList(flagList, os.path.join(main_path, "flag.new"))
    writeFileByList(sub_dim, os.path.join(sub_path, dim_date + ".dim"))
    writeFileByList(flagList, os.path.join(sub_path, "flag.new"))
    
def convert_text_main(text_content, convert_path):
    #入参:单个文本对话的所有文本(字典),json输出位置
    text_total = []
    tab = " " * 4
    text_total.append("[")
    count = 0
    if text_content is None:
        return False
    for dic in text_content:
        count = count + 1
        text_total.append(tab + "{")
        for k, v in dic.items():
            if type(v) == "NoneType":
                v = "none"
        text_total.append(tab * 2 + "\"role\":\"" + dic["ROLENAME"].replace("\"", "") + "\",")
        text_total.append(tab * 2 + "\"datetime\":\"" + dic["COMM_TIME"] + "\",")
        text_total.append(tab * 2 + "\"message\":\"" + dic["MESSAGE_TEXT"].replace("\"", "'").replace("\n", "") + "\"")
        
        if count == len(text_content):
            text_total.append(tab + "}")
        else:
            text_total.append(tab + "},")
    text_total.append("]")
    writeFileByList(text_total, convert_path)
    
    
def writeFileByList(list, filepath):
    #入参:字符串list 写入文本路径
    #功能:生成目标文本
    #特点:通过换行每行写一个元素,主要用于生成json/dim
    if (len(list) == 0):
        return False
    # 分割出目录与文件
    file_path = os.path.split(filepath)
    if (not os.path.exists(file_path[0])):
        os.makedirs(file_path[0])

    out = codecs.open(filepath, 'w')#,encoding='utf8')
    for line in list:
        out.write(str(line).replace("none", "") + '\n')
    out.close()
    
def worker(txtpath,dimpath,dimdate,robotID,systemID,torder):
    line_list = read_file(txtpath,torder) 
    group_dic = group_data(line_list, KEY='task_id')#首行处理
    datas = intergrate(group_dic)
    make_datasource(datas, dimpath, dimdate,robotID,systemID)
    
def pretreatment():
    T_HEAD = "\033[31m "
    T_FOOT = " \033[0m"
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    print " "
    print "*"*75
    print " 注:txt由样例数据格式excel文件去掉表头(只留数据)以制表符分隔得到"
    print " "
    txt_path = raw_input(" 请输入txt文件路径: ")
    data_path=raw_input(" 请输入文本数据源生成根目录: ")
    data_date=raw_input(" 请输入文本数据源生成日期, 格式样例为 202x-xx-xx, 默认今天( "+today+" ): ")
    if data_date == "" or len(data_date) == 0 :
        data_date = today
    data_robot=raw_input(" 请输入机器人客服ID(如果没有,则默认为 '机器人'): ")
    if data_robot == "" or len(data_robot) == 0:
        data_robot = "机器人"
    
    data_kefu=raw_input(" 请输入客服ID (默认值为 '客服'): ")
    if data_kefu == "" or len(data_kefu) == 0:
        data_kefu = "客服"
    data_order=raw_input(" 请按顺序输入 对话ID,角色,对话时间,对话内容 所在列的列数(默认:1,2,3,4): ")
    if data_order == "" or len(data_order) == 0:
        data_order = "1,2,3,4"
    print " "
    print T_HEAD+"请确认以下文本数据源参数是否正确:"+T_FOOT
    print T_HEAD+"制表符分隔且无表头的文本txt路径为:  "+txt_path+T_FOOT
    print T_HEAD+"文本数据源生成根目录为: "+data_path+T_FOOT
    print T_HEAD+"文本数据源生成日期为: "+data_date+T_FOOT
    print T_HEAD+"机器人客服ID为: "+data_robot+T_FOOT
    print T_HEAD+"客服ID为: "+data_kefu+T_FOOT
    print T_HEAD+"对话ID,角色,对话时间,对话内容  分别在txt的 第 "+data_order+" 列"+T_FOOT
    print " "
    my_choose=raw_input("确认以上数据源参数无误 Y/N: ")
    if my_choose == "y" or my_choose == "Y":
        print " "
        print "开始制作数据源,请等待,完成后会有提示"
        print " "
        print " "
        worker(txt_path,data_path,data_date,data_robot,data_kefu,data_order)
        print " "
        print "数据源制作完成，路径在："+data_path
        print " "
        print "*"*75
        print " "
if __name__ == '__main__':
    '''
        参数说明: txt目录  文本数据源路径  dim生成日期  机器人客服ID 客服ID
        参数举例:  /home/txt/1.txt  /home/text  2021-08-04  机器人  坐席
    '''
    #worker("D:\\pachira\\20210623.txt",  "D:\\pachira\\gogndan-ceshi",  "2021-08-04",  "机器人",  "坐席")
    """
    if len(sys.argv)<6:
        print "参数有误,请检查 传入参数,顺序为:"
        print "参数说明: 固定格式excel转存txt目录  文本数据源路径  dim生成日期  机器人客服ID 客服ID"
        print "参数举例:  /home/txt/1.txt  /home/text  2021-08-04  机器人  坐席"
    else:
        worker(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])
    """
    pretreatment()
    