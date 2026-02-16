# FROM alpine:latest
# RUN apk add nodejs npm

FROM node:20-alpine


COPY package*.json ./ 
RUN npm install


COPY . .


ENTRYPOINT ["node", "index"]

# CMD ["nodemon", "index"]



