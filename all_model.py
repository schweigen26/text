from pymysql import connect
from xlwt import Workbook
import re
from time import sleep
import os

class MakeModel(object):
    def __init__(self,info2,project):
        self.project = project
        self.info2 = info2
        # self.dname = "POC模型导入模板"
        self.wb = Workbook(encoding='utf-8')
        self.ws = self.wb.add_sheet('%s.xls' % self.project[:26])

        self.ws.write(0,0,label="行业名称")
        self.ws.write(0,1,label="项目名称")
        self.ws.write(0,2,label="模型专题")
        self.ws.write(0,3,label="所属模型组")
        self.ws.write(0,4,label="模型名称")
        self.ws.write(0,5,label="文本模型")
        self.ws.write(0,6,label="备注")

    def run(self):
        n = 1
        for a in self.info2:
            ret = re.search(r'.*\[(.*)\]', str(a[2]))
            if ret is not None:
                self.ws.write(n,1,label=self.project)
                self.ws.write(n, 3, label=a[0])
                self.ws.write(n, 4, label=a[1])
                self.ws.write(n, 5, label=ret.group(1))
                n +=1
            else:
                continue
        self.wb.save('%s.xls' % self.project[:26])
        print("%s.xls 生成成功！" % self.project[:26])



class SelectMysql(object):
    def __init__(self,ip):
        x = True
        while x:
            try:
                # self.host = input("请输入服务器IP地址(默认：192.168.131.110)：") or "192.168.131.110"
                self.host = ip
                # self.user = input("请输入数据库用户名：")
                # self.password = input("请输入用户密码：")
                # self.database = input("请输入要登陆的数据库：")
                self.conn = connect(host=self.host,port=3306,user='root',password='admin',charset='utf8')
                # self.conn = connect(host='192.168.131.110', port=3306, user='root', password='admin', database='qianxun_debang',charset='utf8')
            except Exception as e:
                print(e)
            else:
                self.csl = self.conn.cursor()
                x = False

    def __del__(self):
        self.csl.close()
        self.conn.close()

    def run(self):
        # 显示所有数据库
        self.csl.execute("""show databases;""")
        info1 = self.csl.fetchall()
        print("*" * 50)

        print("显示全部数据库：")
        for temp1 in info1:
            # print(temp1)
            self.database = temp1[0]


            print("*" * 50)

            # 选择数据库
            # self.database = input("请输入选择的数据库名称：")
            self.conn.select_db(self.database)

            print("*" * 50)
            # print("模型查询结果：")

            # 返回模型内容
            # table_name = input("请输入要查询的表名：")
            # sql = """select name,formula from %s;""" % table_name
            sql = """select g.name,m.name,formula from model m
                        join model_group g on m.model_group_id=g.id
                        where model_group_id is not NULL;"""

            try:
                self.csl.execute(sql)
            except:
                pass
            else:
                info2 = self.csl.fetchall()

                makemodel = MakeModel(info2,self.host[-3:]+self.database)
                makemodel.run()
                # break

def main():
    if os.path.isdir("shujukuallmodel"):
        pass
    else:
        os.mkdir("shujukuallmodel")
    os.chdir("./shujukuallmodel")

    for i in ("192.168.131.110","192.168.131.101","192.168.131.100"):
        select = SelectMysql(i)
        info2 = select.run()


    # time_left = 60
    # while time_left > 0:
    #     print("还有 %d 秒退出！" % time_left)
    #     sleep(1)
    #     time_left -= 1

if __name__ == "__main__":
    main()

