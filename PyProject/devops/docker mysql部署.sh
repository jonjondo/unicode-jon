docker mysql部署
mkdir -p /home/starrocks/opt/mysql/{data,conf,logs}
sudo chown -R 999:999 /home/starrocks/opt/mysql


cat > /home/starrocks/opt/mysql/conf/my.cnf << 'EOF'
[mysqld]
# 基础设置
port = 3306
datadir = /var/lib/mysql
socket = /var/run/mysqld/mysqld.sock

# 字符集设置
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci

# 连接设置
max_connections = 200
max_connect_errors = 1000

# 日志设置
log-error = /var/log/mysql/error.log
slow_query_log = 1
slow_query_log_file = /var/log/mysql/slow.log
long_query_time = 2

# InnoDB 设置
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT

# 二进制日志（可选，用于主从复制和数据恢复）
# server-id = 1
# log-bin = mysql-bin
# binlog_format = ROW
# expire_logs_days = 7

[mysql]
default-character-set = utf8mb4

[client]
default-character-set = utf8mb4
EOF
mysql -h192.168.6.11 -uroot -pdax0jab8mqd@HC

# =================== my.cnf

version: '3.8'

services:
  mysql:
    image: mysql:8.0
    container_name: mysql
    restart: always
    environment:
      # 设置root密码
      MYSQL_ROOT_PASSWORD: dax0jab8mqd@HC
      # 创建默认数据库
      # MYSQL_DATABASE: myapp
      # 可选：创建应用用户
      MYSQL_USER: hive
      MYSQL_PASSWORD: eku7_PQT6rpj
    ports:
      - "3306:3306"
    volumes:
      # 数据持久化
      - /home/starrocks/opt/mysql/data:/var/lib/mysql
      # 配置文件挂载
      - /home/starrocks/opt/mysql/conf/my.cnf:/etc/mysql/conf.d/my.cnf
      # 日志目录挂载
      - /home/starrocks/opt/mysql/logs:/var/log/mysql
    command:
      - --default-authentication-plugin=mysql_native_password
      - --character-set-server=utf8mb4
      - --collation-server=utf8mb4_unicode_ci
    networks:
      - mysql-network
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-p$$MYSQL_ROOT_PASSWORD"]
      interval: 10s
      timeout: 5s
      retries: 3

networks:
  mysql-network:
    driver: bridge