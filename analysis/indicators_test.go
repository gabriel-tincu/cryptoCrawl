package analysis

import (
	"testing"
	"fmt"
)

func TestSeriesMA(t *testing.T) {
	prices := []float64{1,2,3,4,5,6,7,8}
	volumes := []float64{0,0,0,0,0,0,0,0}
	testData := MakeTestData(prices, volumes)
	sma := testData.SMA(4)
	if sma[0].Value != 2.5 {
		t.Fail()
	}
	if sma[4].Value != 6.5 {
		t.Fail()
	}
	ema := testData.SMA(4)
	if ema[0].Value != 2.5 {
		t.Fail()
	}
	if ema[4].Value != 6.5 {
		t.Fail()
	}
}

func TestSeriesMACD(t *testing.T) {
	prices := []float64{10,10,10,10,10,10,10,7,5,3,1,3,5,7,10}
	volumes := make([]float64, len(prices))
	testData := MakeTestData(prices, volumes)
	// TODO -> this "seems" right.... but how to actually test it??
	macd := testData.MACD(3, 6)
	fmt.Println(macd)
	fmt.Println(testData[5:])
}