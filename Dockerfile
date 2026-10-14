FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev || npm install --omit=dev --no-package-lock
COPY server.js ./
# Default port 7860 = Hugging Face Spaces Docker port. Hosts that inject
# their own PORT (Fly.io sets 8080 via fly.toml [env], HF sets 7860) override
# this at runtime — server.js reads process.env.PORT.
ENV PORT=7860
EXPOSE 7860
CMD ["node", "server.js"]
