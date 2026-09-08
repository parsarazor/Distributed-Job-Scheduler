package service

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func nowUTC() time.Time { return time.Now().UTC() }

func toTimestamp(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }
