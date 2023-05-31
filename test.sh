cd redis-tester/
export CODECRAFTERS_SUBMISSION_DIR=/home/adhy/Downloads/codecrafters-redis-python
export CODECRAFTERS_CURRENT_STAGE_SLUG=ping-pong
go run cmd/tester/main.go
export CODECRAFTERS_CURRENT_STAGE_SLUG=ping-pong-multiple
go run cmd/tester/main.go
export CODECRAFTERS_CURRENT_STAGE_SLUG=concurrent-clients
go run cmd/tester/main.go
export CODECRAFTERS_CURRENT_STAGE_SLUG=echo
go run cmd/tester/main.go
export CODECRAFTERS_CURRENT_STAGE_SLUG=set_get
go run cmd/tester/main.go
export CODECRAFTERS_CURRENT_STAGE_SLUG=expiry
go run cmd/tester/main.go
