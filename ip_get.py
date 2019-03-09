import requests,multiprocessing
from multiprocessing  import Pool,Manager
from lxml import etree
import mysql.connector
#实现代理
#储存ip到数据库
#创建数据库，表格
cnx=mysql.connector.connect(user='root',host='localhost',password='123456')
cursor=cnx.cursor()
filename='ip_db'
try:
    cursor.execute('USE {}'.format(filename))
except mysql.connector.Error as err:
    cursor.execute('CREATE DATABASE {} DEFAULT CHARACTER SET=utf8'.format(filename))
cnx.database=filename
table={}
table['ip_table']='''
    CREATE TABLE IF NOT EXISTS ip_table(
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    ip VARCHAR(40) NOT NULL   
    )ENGINE=INNODB CHARSET=UTF8 '''
cursor.execute(table['ip_table'])

#处理页面实体
def get_proxy(q):
    cnx=mysql.connector.connect(user='root',host='localhost',password='123456',database=filename)
    cursor=cnx.cursor()
    for i in range(100):
        if q is not None:
            proxy_entity=q.get()
            proxy_html=etree.HTML(proxy_entity.text)
            ip_trs=proxy_html.xpath('//*[@id="list"]/table/tbody/tr')
            for ip_tr in ip_trs:
                ip=ip_tr[0].text+':'+ip_tr[1].text
                statement=("INSERT INTO ip_table (ip) VALUES (%s)")
                #ip后加',' 构成tuple
                cursor.execute(statement,(ip,))
                cnx.commit()
                #判断是否存入
                #print('{} has been stored'.format(ip))
        else:
            input()
            break
        print('储存第{}页到数据库'.format(i))
#定义初始页
def get_response(q):
    for i in range(100):
        #尝试获取页面
        try:
            proxy_entity=requests.get('https://www.kuaidaili.com/free/intr/{}/'.format(i))
        #网络错误    
        except requests.exceptions.ConnectionError:
            print('获取代理失败，检查网络')
            break
        #存储链接页面
        else:
            if proxy_entity.text=='Invalid Page':
                break
            q.put(proxy_entity)
    q.put(None)

      
#爬取代理链接
if __name__=='__main__':
    q=multiprocessing.Queue()
    r=multiprocessing.Process(target=get_response,args=(q,))
    p=multiprocessing.Process(target=get_proxy,args=(q,))
    r.start()
    p.start()
    r.join()
    p.join()
    
            
            

