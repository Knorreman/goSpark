# Build the same release from its immutable source commit: the former Quay
# image tag is no longer anonymously pullable. No registry credentials needed.
FROM docker.io/golang:1.24-alpine AS build
RUN GOBIN=/out go install github.com/minio/minio@703f51164d3d0c44af41b0d86075a1f61e4779e7
FROM docker.io/alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=build /out/minio /usr/local/bin/minio
ENTRYPOINT ["minio"]
