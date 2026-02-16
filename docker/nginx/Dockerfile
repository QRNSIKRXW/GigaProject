FROM nginx:latest

# Копируем конфиг
COPY docker/nginx/nginx.conf /etc/nginx/nginx.conf

# Копируем фронтенд
COPY frontend /usr/share/nginx/html
