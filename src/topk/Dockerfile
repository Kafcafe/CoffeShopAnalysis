FROM golang:1.25.1-alpine3.21 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
# First stage is used to compile golang binary and second stage is used to only copy the 
# binary generated to the deploy image. 
# Docker multi stage does not delete intermediate stages used to build our image, so we need 
# to delete it by ourselves. Since docker does not give a good alternative to delete the intermediate images
# we are adding a very specific label to the image to then find these kind of images and delete them
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

# Pre-cache dependencies
COPY ./topk/go.mod ./topk/go.sum ./
COPY ./common ../common
COPY ./group ../group

RUN go mod download

COPY ./topk .
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/topk .


FROM busybox:latest
COPY --from=builder /build/bin/topk /topk
RUN chmod +x /topk
ENTRYPOINT ["/topk"]
