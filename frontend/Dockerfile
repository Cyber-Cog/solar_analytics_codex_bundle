# syntax=docker/dockerfile:1.6
#
# Solar Analytics frontend image (nginx reverse proxy).
#
# Serves the static UI directly and forwards /api/, /auth/, /docs, /openapi.json
# to the FastAPI backend container. Keeps the main Python process focused on
# dynamic work instead of shipping JS/CSS bytes on every request.

FROM nginx:1.27-alpine

COPY frontend /usr/share/nginx/html
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
