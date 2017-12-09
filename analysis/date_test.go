package analysis

import (
	"testing"
	"time"
)

func TestGroup(t *testing.T) {
	now := time.Now()
	var testData Series
	for i:=0; i < 1000; i++ {
		sample := Sample{10., now.Add(time.Second*time.Duration(i))}
		testData = append(testData, sample)
	}
	d, _ := time.ParseDuration("5s")
	grouped := testData.GroupByTime(d)
	if len(grouped) != 200 {
		t.Fatalf("length should be 200 but is %d", len(grouped))
	}
	for _, g := range grouped {
		if len(g) != 5 {
			t.Fatalf("length should be 5 but is %d", len(g))
		}
	}
	for _, s := range testData.SMA(10) {
		if s.Value != 10 {
			t.Fatalf("value should be 10")
		}
	}
	for _, s := range testData.EMA(10) {
		if s.Value != 10 {
			t.Fatalf("value should be 10")
		}
	}
	for _, s := range testData.MACD(10, 20) {
		if s.Value != 0 {
			t.Fatalf("value should be 0")
		}
	}
}