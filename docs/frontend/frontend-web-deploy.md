# Deploying TrackSanté Web Frontend (`app.tracksante.com`)

## Overview
The Flutter web app is served at:

- `https://app.tracksante.com` (Flutter web frontend)
- via **Cloudflare Tunnel** → `localhost:8089`
- with **Nginx (Docker)** serving static files from `/srv/tracksante-app`

---

## 1) Docker Compose service (Nginx for Flutter web)

```yaml
tracksante-app:
  image: nginx:alpine
  restart: unless-stopped
  volumes:
    - /srv/tracksante-app:/usr/share/nginx/html:ro
    - ./tracksante-app.conf:/etc/nginx/conf.d/default.conf:ro
  ports:
    - "8089:80"
````

---

## 2) Nginx config (`tracksante-app.conf`)

```nginx
server {
    listen 80;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location = /index.html {
        add_header Cache-Control "no-store";
    }

    location /assets/ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
```

This `try_files` line is important for Flutter web routing.

---

## 3) Server folder for web build

```bash
sudo mkdir -p /srv/tracksante-app
sudo chown -R $USER:$USER /srv/tracksante-app
```

---

## 4) Cloudflare Tunnel (`/etc/cloudflared/config.yml`)

Add this ingress rule:

```yaml
- hostname: app.tracksante.com
  service: http://localhost:8089
```

Then validate + restart:

```bash
cloudflared tunnel ingress validate
cloudflared tunnel ingress rule https://app.tracksante.com
sudo systemctl restart cloudflared
```

---

## 5) Cloudflare DNS

In Cloudflare DNS, add:

* **Type:** `CNAME`
* **Name:** `app`
* **Target:** `<TUNNEL-UUID>.cfargotunnel.com`

---

## 6) Manual deploy (first test)

Build locally and upload:

```bash
flutter build web --release
rsync -av --delete build/web/ <user>@<server>:/srv/tracksante-app/
```

Check base path is correct:

```bash
grep -n "base href" /srv/tracksante-app/index.html
```

Should be:

```html
<base href="/">
```

---

## 7) Fix login redirect to app (API `.env`)

The API callback was redirecting to `tracksante.com`. Fixed with:

```env
WEB_ORIGIN=https://app.tracksante.com
AUTH_PUBLIC_BASE=https://auth.tracksante.com
AUTH_REDIRECT_URI_WEB=https://api.tracksante.com/auth/oidc/callback
AUTH_SUCCESS_REDIRECT=https://app.tracksante.com/
AUTH_SUCCESS_REDIRECT_PATIENT=https://app.tracksante.com/
AUTH_SUCCESS_REDIRECT_DOCTOR=https://app.tracksante.com/doctor
AUTH_OIDC_SCOPES="openid email profile groups"
AUTH_DOCTOR_GROUPS=doctor,doctors
```

`AUTH_SUCCESS_REDIRECT_DOCTOR` is used automatically by the API callback when Authentik resolves the user as a doctor and the frontend did not pass an explicit `next` query parameter to `/auth/login`.

For the frontend boot flow, call `/auth/refresh` or `/auth/me` and read:

- `portal`: `doctor` or `patient`
- `is_doctor`: boolean convenience flag
- `default_redirect_path`: route-safe path such as `/doctor`

For native mobile, use `/auth/mobile/exchange` once after the PKCE callback/deep link to receive the TrackSanté refresh token, then store that refresh token securely in the app and use `/auth/refresh` with `{ "refresh_token": "..." }` for subsequent app boots.

Restart API:

```bash
docker compose up -d api
```

---

## 8) Auto-deploy with self-hosted GitHub Runner (`gh-runner`)

The runner builds on the server and copies files directly into `/srv/tracksante-app`.

### Permission setup (one-time)

```bash
sudo groupadd -f tracksante-deploy
sudo usermod -aG tracksante-deploy rishit
sudo usermod -aG tracksante-deploy gh-runner
sudo chown -R rishit:tracksante-deploy /srv/tracksante-app
sudo chmod -R 2775 /srv/tracksante-app
sudo systemctl restart actions.runner.TrackSante490.capstone-server.service
```

### Working deploy step in GitHub Actions

(`rsync -a` caused `chgrp` permission errors, so this version is used)

```yaml
- name: Deploy to local web folder
  run: |
    rsync -rv --delete \
      --no-owner --no-group --no-perms \
      --omit-dir-times \
      build/web/ /srv/tracksante-app/
```

---

## 9) Quick checks

```bash
docker compose up -d tracksante-app
curl -I http://localhost:8089
curl -I https://app.tracksante.com
```

If those return `200`, the app is being served correctly.

---
