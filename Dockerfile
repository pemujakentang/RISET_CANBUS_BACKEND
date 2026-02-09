# ===== Build stage =====
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Copy prisma schema
COPY prisma ./prisma

# Install all deps (termasuk devDeps untuk build)
RUN npm install

# Copy source code
COPY . .

# Generate Prisma client
RUN npx prisma generate

# Build TypeScript -> dist/
RUN npm run build


# ===== Runtime stage =====
FROM node:20-alpine

WORKDIR /app

ENV NODE_ENV=production

# Copy only package files
COPY package*.json ./

# Install only production deps
RUN npm install --omit=dev

# Copy Prisma client & schema
COPY --from=builder /app/prisma ./prisma
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma

# Copy compiled output
COPY --from=builder /app/dist ./dist

EXPOSE 4000

CMD ["npm", "run", "start"]
