FROM node:20-slim

WORKDIR /app

# Install overlay-express pinned to the exact mainline version the harness
# targets. `pg` is needed because knex does not auto-install DB drivers.
RUN npm init -y >/dev/null \
 && npm install --omit=dev \
      @bsv/overlay-express@2.2.0 \
      mysql2@^3

COPY server.mjs .

EXPOSE 8080

CMD ["node", "server.mjs"]
