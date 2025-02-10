ARG PLATFORM=linux 
ARG GOOS=linux
ARG GOARCH=x86_64

FROM --platform=$PLATFORM golang:1.23

WORKDIR /app

RUN git clone -b pixxel --single-branch https://github.com/pixxelhq/BemiDB.git .

WORKDIR /app/src
RUN go mod download
RUN CGO_ENABLED=1 GOOS=$GOOS GOARCH=$GOARCH go build -o /app/bemidb

FROM debian:12.9-slim
RUN apt update && apt-get install -y ca-certificates

WORKDIR /app

COPY --from=0 /app/bemidb .

CMD ["/app/bemidb", "start"]
