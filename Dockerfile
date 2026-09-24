FROM golang:1.26-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libsystemd-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /out/vm-agent .

# Deliberately NOT distroless (the one deviation from docker-agent's/
# k8s-agent's image pattern): this agent reads the systemd journal via
# cgo + libsystemd (sdjournal), which needs libsystemd.so.0 present at
# runtime -- a distroless base has no package manager and no libsystemd,
# so a real Debian slim base is required here specifically.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libsystemd0 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/vm-agent /vm-agent
# Deliberately root, same justification as docker-agent's own Dockerfile:
# this agent reads host-owned files (the bind-mounted journal, /var/log,
# /proc, and the root filesystem for storage stats) whose ownership varies
# host-to-host and can't be matched at build time. No other host mount,
# no --privileged, no extra capabilities beyond what those read-only
# mounts already grant.
ENTRYPOINT ["/vm-agent"]
