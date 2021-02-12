FROM node:14-alpine

RUN mkdir -p /home/node/app
WORKDIR /home/node/app


# Copy package.json
COPY package*.json /home/node/app/

# install dependecies
RUN apk update && \
    # Install git
    apk add --no-cache bash git openssh && \
    # Install node-gyp dependencies
    apk add --no-cache make gcc g++ python && \
    # npm install
    npm ci --only=production && \
    # Uninstall git
    apk del bash git openssh && \
    # Uninstall node-gyp dependencies
    apk del make gcc g++ python

# Copy working files
COPY index.js /home/node/app/

USER nobody

# Run the js script
CMD ["node", "--max_old_space_size=256", "index.js"]

