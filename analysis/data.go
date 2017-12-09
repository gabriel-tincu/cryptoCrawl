package analysis

import (
	"time"
)

type Sample struct {
	Value float64
	Time  time.Time
}

type OHLC struct {
	Time  time.Time
	Open  float64
	High  float64
	Low   float64
	Close float64
}

type Series []Sample
type GroupedSeries []Series

type AggregateFunc func(s Series) Sample

func (s *Series) GroupByTime(duration time.Duration) (response GroupedSeries) {
	if s == nil {
		return nil
	}
	if len(*s) == 0 {
		return
	}
	start := (*s)[0]
	current := Series{start}
	for _, sample := range (*s)[1:] {
		if start.Time.Add(duration).Sub(sample.Time) > 0 {
			current = append(current, sample)
		} else {
			response = append(response, current)
			start = sample
			current = Series{start}
		}
	}
	if len(current) > 0 {
		response = append(response, current)
	}
	return response
}
