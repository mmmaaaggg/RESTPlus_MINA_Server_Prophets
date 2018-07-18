# RESTPlus_MINA_Server_Prophets
通过Flask-RESTPlus 构建微信小程序 服务器端 实现预言家网站应用
包含服务期端，celery后台worker、beat

### 激活 venv
windows 下
```commandline
python3 -m venv ./venv 
.\venv\Scripts\activate.bat
pip3 install -r requirements.txt
```
部分模块可能会安装失败，需要下载wheel文件手动安装

linux 下
```bash
python3 -m venv ./venv 
source ./venv/bin/activate
pip3 install -r requirements.txt
```

### 启动 App
```bash
python3 run.py
```

### 启动 Celery worker
```bash
celery -A celery_task.task.celery worker --loglevel=info
```

### 启动 Celery beat
```bash
celery -A celery_task.task.celery beat --loglevel=info
```