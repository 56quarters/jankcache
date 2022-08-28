package core

import "time"

type Time interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) *time.Timer
}

type DefaultTime struct{}

func (t DefaultTime) Now() time.Time {
	return time.Now()
}

func (t DefaultTime) AfterFunc(d time.Duration, f func()) *time.Timer {
	return time.AfterFunc(d, f)
}
