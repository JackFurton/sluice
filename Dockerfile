# Build the manager, the worker, and the fake vendor API in one image.
#
# The worker ships alongside the manager so an IngestionSource that names no
# image still gets a run pod that works: the controller points run pods at its
# own image and overrides the entrypoint. The fake API is here so the kind demo
# and the e2e suite need nothing from a registry beyond this one build.
FROM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o worker cmd/worker/main.go
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o fakeapi cmd/fakeapi/main.go

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/worker .
COPY --from=builder /workspace/fakeapi .
USER 65532:65532

ENTRYPOINT ["/manager"]
