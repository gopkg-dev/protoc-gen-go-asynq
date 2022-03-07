.PHONY: proto
# generate proto
proto:
	protoc --proto_path=. \
		   --go_out=paths=source_relative:. \
		   asynq/asynq.proto

.PHONY: test
test:
	go install . && \
	protoc --proto_path=. \
		   --go_out=paths=source_relative:. \
		   --go-asynq_out=paths=source_relative:. \
		   example/example.proto
