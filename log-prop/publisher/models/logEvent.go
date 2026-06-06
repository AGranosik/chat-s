package models

import "time"

type LogEvent struct {
	Level   string    `json:"level"`
	Message string    `json:"msg"`
	Service string    `json:"service"`
	Time    time.Time `json:"time"`
}
