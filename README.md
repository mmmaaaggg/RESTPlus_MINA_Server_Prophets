# RESTPlus_MINA_Server_Prophets
通过Flask-RESTPlus 构建微信小程序 服务器端 实现预言家网站应用
包含服务期端，celery后台worker、beat

### 激活 venv
windows 下
```commandline
.\venv\Scripts\activate.bat
```
linux 下
```bash
source ../Prophet/venv/bin/activate
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