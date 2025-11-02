FROM grafana/grafana:latest

# Cài plugin Redis chính thức
RUN grafana-cli plugins install redis-datasource

# Đảm bảo thư mục plugin có quyền truy cập
USER root
RUN chmod -R 777 /var/lib/grafana/plugins
USER grafana
