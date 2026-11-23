# Build:  docker build -t filings-watcher watcher/
# Run:    docker run -p 8080:8080 -v watcher-data:/data -e SEC_USER_AGENT="Your Name your@email" filings-watcher
# Demo:   docker run -p 8080:8080 filings-watcher serve -demo      (made-up filings, no network)
#
# The SEC contact is passed at run time. It is never built into the image.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /watcher ./cmd/watcher \
 && mkdir /data

# The binary is static and the module has no dependencies, so the image is the binary, the CA
# certificates it needs to reach sec.gov over TLS, and an empty data directory owned by the user.
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /watcher /watcher
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/watcher"]
CMD ["serve", "-addr", ":8080", "-data", "/data"]
