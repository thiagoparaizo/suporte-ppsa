# gunicorn.conf.py
workers = 4
bind = "0.0.0.0:5005"
timeout = 1800
worker_class = 'sync'
backlog = 2048
max_requests = 1000
max_requests_jitter = 50
keepalive = 65

# Configurações de buffer
forwarded_allow_ips = '*'
proxy_protocol = True
proxy_allow_ips = '*'

# Configurações de log
loglevel = 'info'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
accesslog = '-'
errorlog = '-'

# Configurações de Performance
preload_app = True
forwarded_allow_ips = '*'

# Configurações de graceful shutdown
graceful_timeout = 300