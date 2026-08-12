# Build with the toolchain go.mod asks for, so a local Go version cannot make
# the image differ from CI.
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies first: they change far less often than the code, so an edit to a
# handler does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off: the runtime image has no libc to link against, so a dynamically
# linked binary would build here and fail to start there.
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/reconsync ./cmd/reconsync && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/reconsyncctl ./cmd/reconsyncctl

# Distroless rather than alpine: nothing in this image needs a shell, and a
# reconciliation service that holds an audit chain is not a place to leave one
# lying around for whoever gets in.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/reconsync /usr/local/bin/reconsync
COPY --from=build /out/reconsyncctl /usr/local/bin/reconsyncctl

# Migrations travel with the binary that expects them. A container running one
# schema against another is the failure this avoids.
COPY migrations /migrations

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/reconsync"]
