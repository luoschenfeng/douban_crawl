from multiprocessing.managers import BaseManager
import mysql.connector
import queue,requests,re,json

#电影爬取队列
data_crawl=queue.Queue() 
#储存队列
data_store=queue.Queue()
#自定义函数，在windows中lambda不能被序列化
def return_data_crawl():
    global data_crawl
    return data_crawl
def return_data_store():
    global data_store
    return data_store
#继承BaseManager
class JobManager(BaseManager):
    pass
if __name__ == '__main__':
    #注册两个Queue到网络
    JobManager.register('data_crawl_queue',callable=return_data_crawl)
    JobManager.register('data_store_queue',callable=return_data_store)

    #绑定继承类到端口，检查那个端口可以用
    manager=JobManager(address=('127.0.0.1',2000),authkey=b'abc')

    manager.start()

    #获取网络中的queue
    crawl=manager.data_crawl_queue()
    store=manager.data_store_queue()


    #主页电影名称规范，爬取时首先进行爬取
    #建立数据库
    #数据库名称
    db_name='douban_crawl'
    #创建数据库连接
    cnx=mysql.connector.connect(user='root',password='123456',host='localhost')
    cur=cnx.cursor()
    '''创建数据库'''
    try:
    	cur.execute('USE {}'.format(db_name))
    except:
    	cur.execute('CREATE DATABASE {} DEFAULT CHARACTER SET=UTF8'.format(db_name))
    else:
    	cur.execute('USE {}'.format(db_name))
    #取得连接
    cnx.database=db_name


    #创建表格，以电影id为主键
    tables={}
    tables['movie_first_name']="CREATE TABLE IF NOT EXISTS movie_first_name(id INT PRIMARY KEY,movie_first_name VARCHAR(30) NOT NULL)ENGINE=INNODB;"
    tables['links']="CREATE TABLE IF NOT EXISTS links(id INT PRIMARY KEY,douban_link VARCHAR(50),imdb_link VARCHAR(50),img_link VARCHAR(100))ENGINE=INNODB;"
    tables['director']="CREATE TABLE IF NOT EXISTS director(id INT PRIMARY KEY,director VARCHAR(15) NOT NULL)ENGINE=INNODB;"
    tables['writers']="CREATE TABLE IF NOT EXISTS writers(id INT PRIMARY KEY,first_w VARCHAR(15) NOT NULL,sccond_w VARCHAR(15),third_w VARCHAR(15))ENGINE=INNODB;"
    tables['stars']="CREATE TABLE IF NOT EXISTS stars(id INT PRIMARY KEY,first_s VARCHAR(15) NOT NULL,sccond_s VARCHAR(15),third_s VARCHAR(15),fourth_s VARCHAR(15),fifth_s VARCHAR(15))ENGINE=INNODB;"
    tables['types']="CREATE TABLE IF NOT EXISTS types(id INT PRIMARY KEY,first_T VARCHAR(5) NOT NULL,sccond_T VARCHAR(5),third_T VARCHAR(5))ENGINE=INNODB;"
    tables['times']="CREATE TABLE IF NOT EXISTS times (id INT PRIMARY KEY,first_T VARCHAR(20) NOT NULL,sccond_T VARCHAR(20),third_T VARCHAR(20))ENGINE=INNODB;"
    tables['areas']="CREATE TABLE IF NOT EXISTS areas(id INT PRIMARY KEY,first_a VARCHAR(10) NOT NULL,sccond_a VARCHAR(10),third_a VARCHAR(10))ENGINE=INNODB;"
    tables['languages']="CREATE TABLE IF NOT EXISTS languages(id INT PRIMARY KEY,first_l VARCHAR(10) NOT NULL,sccond_l VARCHAR(10),third_l VARCHAR(10))ENGINE=INNODB;"
    tables['score']="CREATE TABLE IF NOT EXISTS score(id INT PRIMARY KEY,score float(2,1),persons INT )ENGINE=INNODB;"
    tables['movie_other_name']="CREATE TABLE IF NOT EXISTS movie_other_name(id INT PRIMARY KEY,first_n VARCHAR(30),sccond_n VARCHAR(30),third_n VARCHAR(30))ENGINE=INNODB;"
    tables['length']="CREATE TABLE IF NOT EXISTS length(id INT PRIMARY KEY,length SMALLINT)ENGINE=INNODB;"
    #创建表格
    for name,st in tables.items():
        cur.execute(st)
    '''创建完毕'''

    #设置代理
    cnx_i=mysql.connector.connect(user='root',host='localhost',password='123456',database='ip_db')
    cur_proxy=cnx_i.cursor()
    #查询语句，倒叙排序将最新代理放在后面，方便后面将不能用的代理删除，
    query='SELECT (ip) FROM ip_table WHERE id BETWEEN %s AND %s ORDER BY id DESC'
    #前50个代理
    cur_proxy.execute(query,(1,50))
    proxy_list=[]
    for i in cur_proxy:
        #后面要加索引
    	proxy_list.append(i[0])
    #设定循环步数
    proxy_cycle=list(i for i in range(0,50))


    #爬取数据
    for start in range(0,10000,20):
        #对代理ip次数计数
        j=0
        for i in proxy_cycle[::-1]:
                try:
                    print('尝试爬取第{}组'.format(int(start/20)))
                    re_content=requests.get('https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start={}'.format(start),proxies={'https':'http://{}'.format(proxy_list[i]),'http':'http://{}'.format(proxy_list[i-1])})
                    print('获取主页数据成功')
                except:
                    j+=1
                    print('主机尝试更换代理ip-->',proxy_list[i],'  ',proxy_list[i-1])
                    #使得下一次循环时不用其废弃代理
                    proxy_cycle.remove(i)
                    if j>5:
                        print('主机代理替换次数过多，正在退出')
                        break
                    continue
                else:
                    break
        re_data=re_content.json()['data']
        #检测到数据爬取完毕
        if re_data==[]:
            print('爬取已到尽头')
            break
        for data_index in re_data:
            #先做简单的处理
            id=data_index['id']
            #将id进行发送,
            crawl.put(id)
            print('{}已经发送给辅机'.format(id))
            first_name=data_index['title']
            director=data_index['directors'][0]
            print('未出错')
            stars=data_index['casts']
            #将id加入到stars列表中,方便添加
            stars.insert(0,id)
            cnx=mysql.connector.connect(user='root',host='localhost',password='123456',database=db_name)
            cursor=cnx.cursor()
            cursor.execute("INSERT INTO movie_first_name (id,movie_first_name)values (%s,%s)",(id,first_name))
            cursor.execute("INSERT INTO director (id,director)values (%s,%s)",(id,director))
            #(%s,%s,%s,%s,%s,)会导致错误
            cursor.execute("INSERT INTO stars values ({})".format(','.join(['%s']*len(stars[:6]))),tuple(stars[:6]))
            cnx.commit()
            print(id,' ----\n',id,' ->')
    #读取任务
    for start in range(0,10000,20):
        cnx=mysql.connector.connect(user='root',host='localhost',password='123456',database=db_name)
        cursor=cnx.cursor()
        html=store.get()[0]
        #需要重新获得id
        id=store.get()[1]
        #编剧writers
        writers=html.xpath('//*[@id="info"]/span[2]/descendant::a[@href]/text()')
        #writers添加id
        writers.insert(0,id)
        #插入writers
        cursor.execute("INSERT INTO writers values ({})".format('%s,'*len(writers)),tuple(writers[:4]))
        #类型types
        types=html.xpath('//*[@id="info"]/child::span[@property="v:genre"]/text()')
        #types添加id
        types.insert(0,id)
        #插入types
        cursor.execute("INSERT INTO types values ({})".format('%s,'*len(types)),tuple(types[:4]))
        #imdb链接
        imdb_link=html.xpath('//*[@id="info"]/a[last()]/@href')[0]
        #豆瓣链接
        douban_link="https://movie.douban.com/subject/{}/".format(id)
        #图片链接
        img_link=html.xpath('//*[@id="mainpic"]/a/img/@src')[0]
        #插入三个link
        cursor.execute("INSERT INTO links values (%s,%s,%s,)",(id,douban_link,imdb_link,img_link))
        #时间time(字符串形式)
        times=html.xpath('//*[@id="info"]/span[@property="v:initialReleaseDate"]/text()')
        #times添加id
        times.insert(0,id)
        #插入times
        cursor.execute("INSERT INTO times values ({})".format('%s,'*len(times)),tuple(times[:4]))
        #地区
        areas_str=html.xpath('//*[@id="info"]/child::span[@property="v:genre"][last()]/following-sibling::span')[0].tail
        #匹配模式'梁朝伟 / 白百何 / 井柏然 / 李宇春 / 杨祐宁 /'
        pattern='[^/ ]+'
        areas=[]
        for match in re.finditer(pattern,areas_str):
        	s=match.start()
        	e=match.end()
        	areas.append(areas_str[s:e])
        #添加id
        areas.insert(0,id)
        #插入数据
        cursor.execute("INSERT INTO areas values ({})".format('%s,'*len(areas)),tuple(areas[:4]))
        #又名
        other_name_str=html_object.xpath('//*[@id="info"]/span[text()="又名:"]')[0].tail
        #匹配模式' 月黑高飞(港) / 刺激1995(台) / 地狱诺言 / 铁窗岁月 / 消香克的救赎'
        movie_other_name=[]
        for match in re.finditer(pattern,other_name_str):
        	s=match.start()
        	e=match.end()
        	movie_other_name.append(other_name_str[s:e])
        #添加id
        movie_other_name.insert(0,id)
        #插入数据
        cursor.execute("INSERT INTO movie_other_name values ({})".format('%s,'*len(movie_other_name)),tuple(movie_other_name[:4]))
        #语言language
        languages_str=html.xpath('//*[@id="info"]/child::span[@property="v:genre"][last()]/following-sibling::span')[1].tail
        #匹配模式'汉语普通话 / 粤语 / 泰语 / 缅甸语 / 英语'
        languages=[]
        for match in re.finditer(pattern,languages_str):
        	s=match.start()
        	e=match.end()
        	languages.append(languages_str[s:e])
        #添加id
        languages.insert(0,id)
        #插入数据
        cursor.execute("INSERT INTO languages values ({})".format('%s,'*len(languages)),tuple(languages[:4]))
        #评分
        score=html_object.xpath('//*[@id="interest_sectl"]/div[@rel="v:rating"]/div[@typeof="v:Rating"]/strong/text()')[0]
        score=float(score)
        #评分人数
        persons=html_object.xpath('//*[@id="interest_sectl"]//a[@href="collections"]/span/text()')[0]
        persons=int(persons)
        #插入数据
        cursor.execute("INSERT INTO persons values (%s,%s,%s,)",(id,score,persons))
        #片长
        length_str=html_object.xpath('//*[@id="info"]/span[@property="v:runtime"]/text()')[0]
        #匹配模式'142分钟'
        pattern=r'\d+'
        match=re.search(pattern, length_str)
        s=match.start()
        e=match.end()
        length=int(length_str[s:e])
        #插入数据
        cursor.execute("INSERT INTO length values (%s,%s,%s,)",(id,length))
        cnx.commit()
        print(id,' ---->\n',id,' ---->')

    #关闭queue
    manager.shutdown()
    #关闭数据库
    cnx.close()





