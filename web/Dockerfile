# Multi-stage: node builds the static bundle, Caddy serves it and proxies the
# API, so the whole dashboard is one container with no Node runtime in prod.
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
# npm ci needs the lockfile; fall back to install so a fresh clone (no lock yet)
# still builds instead of failing the Docker build.
RUN if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi
COPY . .
RUN npm run build

FROM caddy:2-alpine
COPY Caddyfile /etc/caddy/Caddyfile
COPY --from=build /app/dist /srv
EXPOSE 80
