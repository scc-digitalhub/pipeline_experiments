## Nginx Conf for IMDb Pipelines

```

worker_processes  1;

events {
    worker_connections  1024;
}

http {
    include       mime.types;
    default_type  application/octet-stream;

    sendfile        on;
    keepalive_timeout  65;

    server {
        location / {
            root   html;
            index  index.html index.htm;
        }

        location /credits/ {
            root        C:/Users/Erica.Tomaselli/pipelines_scripts/dati-indices-imdb;
            try_files   $uri $uri.json =404;
        }

        location /business/ {
            root        C:/Users/Erica.Tomaselli/pipelines_scripts/dati-indices-imdb;
            try_files   $uri $uri.json =404;
        }
    }

}

```