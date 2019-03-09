from multiprocessing.managers import BaseManager
from lxml import etree
import mysql.connector
import queue

#创建继承
class JobManager(BaseManager):
	pass
#注册
JobManager.register('data_crawl_queue')
JobManager.register('data_store_queue')
#连接主机
server_address='127.0.0.1'
#端口验证
m=JobManager(address=(server_address,2000),authkey=b'abc')
#从网络连接
m.connect()
#从网络获取对象
crawl=m.data_crawl_queue()
store=m.data_store_queue()

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

#从主机队列获取id，处理后给store
for start in range(0,10000,20):
	id=crawl.get()
	#对代理ip次数计数
	j=0
	for i in proxy_cycle[::-1]:
		try:
			re=requests.get('https://movie.douban.com/subject/{}/'.format(id),proxies={'https':'http://{}'.format(proxy_list[i]),'http':'http://{}'.format(proxy_list[i-1])})
			html=etree.HTML(re.text)
			#发送到store进行储存
			print('发送html实体到store')
			store.put([html,id])
			#等待一段时间，防止服务器检测到关闭
			time.sleep(1)
		except:
			j+=1
			print('副机尝试更换代理ip-->',proxy_list[i],'  ',proxy_list[i-1])
			proxy_cycle.remove(i)
			if j>10:
				print('副机代理替换次数过多，正在退出')
				break
			continue
		else:
			break

print('结束')
