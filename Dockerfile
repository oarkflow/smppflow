# Build context must be the PARENT directory containing both repositories,
# because go.mod declares: replace github.com/oarkflow/broker => ../broker
#
#   docker build -f smppflow/Dockerfile -t smppflow:latest .
#
# where `.` contains ./smppflow and ./broker.
FROM golang:1.26 AS build
WORKDIR /src
COPY broker/ /src/broker/
COPY smppflow/ /src/smppflow/
WORKDIR /src/smppflow
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/smppflow-server ./examples/server
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/smppflow-client ./examples/client

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build --chown=65532:65532 /out/smppflow-server /smppflow-server
COPY --from=build --chown=65532:65532 /out/smppflow-client /smppflow-client
COPY --chown=65532:65532 smppflow/routes.demo.json /etc/smppflow/routes.json

# SMPP listener and HTTP API.
EXPOSE 2775 8080

# Data directory for --queue-backend=broker WAL persistence. Mount a volume here.
RUN mkdir -p /data && chown 65532:65532 /data
VOLUME ["/data"]

USER 65532:65532
ENTRYPOINT ["/smppflow-server"]
CMD ["--http", ":8080", "--smpp", ":2775", "--provider-addr", "127.0.0.1:2775", "--routes", "/etc/smppflow/routes.json", "--queue-backend", "memory"]
