# Frontend Dockerfile - React + Vite Development Mode
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY src/package*.json ./

# Install dependencies
# First do a clean install, then explicitly install the rollup binary for musl
RUN npm install --force && \
    npm install --force @rollup/rollup-linux-x64-musl

# Copy source code
COPY src/ ./

# Expose Vite dev server port
EXPOSE 3000

# Start dev server with hot reload
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
