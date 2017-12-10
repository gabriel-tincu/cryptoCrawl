package analysis

import (
	"testing"
	"time"
)

func MakeTestData(prices, volumes []float64) (response Series) {
	now := time.Now()
	for i := 0; i < len(prices); i++ {
		response = append(response, Sample{Volume: volumes[i], Value: prices[i], Time: now.Add(time.Second * time.Duration(i))})
	}
	return
}

func TestGroup(t *testing.T) {
	prices := make([]float64, 1000)
	volumes := make([]float64, 1000)
	testData := MakeTestData(prices, volumes)
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
}
