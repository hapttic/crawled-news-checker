FROM node:18-alpine

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./

# Install dependencies
RUN npm install

# Bundle app source
COPY . .

# Set environment variables with empty values (to be provided at runtime)
ENV NODE_ENV=production
# AWS Configuration
ENV AWS_REGION=""
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV S3_BUCKET=""
# MongoDB Configuration
ENV MONGODB_URI=""
ENV DB_NAME=""
ENV COLLECTION_NAME=""

# Create a non-root user to run the application
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
RUN chown -R appuser:appgroup /usr/src/app
USER appuser

# Expose port if needed for future web interface
EXPOSE 3000

# Set the command to run in cron mode by default
CMD ["node", "src/index.js", "cron"] 