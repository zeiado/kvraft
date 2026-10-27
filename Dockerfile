FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/kvserver ./cmd/kvctl

FROM alpine:3.21
RUN adduser -D -H kvraft && mkdir /data && chown kvraft /data
COPY --from=build /out/ /usr/local/bin/
USER kvraft
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["kvserver"]
