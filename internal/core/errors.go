package core

import "errors"

var (
	ErrQueueExists   = errors.New("queue already exists")
	ErrQueueNotFound = errors.New("queue not found")
	ErrLeaseLost     = errors.New("lease expired or token mismatch")
	ErrNameReserved  = errors.New("queue names ending in .dlq are reserved")
	ErrInvalid       = errors.New("invalid request")
)
