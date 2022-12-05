# syntax=docker/dockerfile:1
FROM golang:1.19 as builder
ARG token

ENV GOPRIVATE=github.com/bloXroute-Labs/*
RUN git config --global url.https://$token@github.com/.insteadOf https://github.com/

WORKDIR /build
COPY . /build/
RUN --mount=type=cache,target=/root/.cache/go-build make build-for-docker

FROM alpine

RUN apk add --no-cache libgcc libstdc++ libc6-compat
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/mev-boost /app/mev-boost
COPY ./static /app/static
EXPOSE 18550
ENTRYPOINT ["/app/mev-boost"]
