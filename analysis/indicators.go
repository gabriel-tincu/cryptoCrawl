package analysis

import (
	"math"
	"time"
)

const (
	epsilon = 0.000000001
)

func (s *Series) Normalize() Series {
	grouped := s.GroupByTime(time.Second)
	return grouped.Mean()
}

func (g *GroupedSeries) Mean() Series {
	var resp Series
	for _, s := range *g {
		resp = append(resp, s.Mean())
	}
	return resp
}

func (g *GroupedSeries) OHLC() []OHLC {
	var response []OHLC
	var max, min float64
	for _, s := range *g {
		max, min = s[0].Value, s[0].Value
		for _, sample := range s {
			max = math.Max(max, sample.Value)
			min = math.Min(min, sample.Value)
		}
		o := OHLC{
			Time:  s[0].Time,
			Close: s[len(s)-1].Value,
			Open:  s[0].Value,
			High:  max,
			Low:   min,
		}
		response = append(response, o)
	}
	return response
}

func (s *Series) MACD(short, long int) Series {
	var response Series
	longSma := s.SMA(long)
	shortSma := s.SMA(short)
	for i := 0; i < len(longSma); i++ {
		response = append(response, Sample{Time: shortSma[i].Time, Value: shortSma[i].Value - longSma[i].Value})
	}
	return response
}

func (s *Series) SMA(periodCount int) Series {
	resp := Series{}
	for i := 0; i < len(*s)-periodCount; i++ {
		slice := (*s)[i : i+periodCount]
		resp = append(resp, (&slice).Mean())
	}
	return resp
}

func (s *Series) EMA(periodCount int) Series {
	percentage := 2. / float64(periodCount+1)
	var response Series
	for i := 0; i < len(*s)-periodCount; i++ {
		subSeries := (*s)[i : i+periodCount]
		if i == 0 {
			response = append(response, subSeries.Mean())
			continue
		}
		closingSample := subSeries[len(subSeries)-1]
		response = append(response, Sample{Value: closingSample.Value*percentage + (1-percentage)*response[i-1].Value, Time: subSeries[0].Time})
	}
	return response
}

func(s *Series) RSI(periodCount int) Series {
	var up, down, response Series
	for i := 0; i < len(*s) ; i++ {
		if i == 0 {
			continue
		}
		current := (*s)[i]
		previous := (*s)[i-1]
		if current.Value > previous.Value {
			up = append(up, Sample{Time:current.Time, Value:current.Value-previous.Value})
			down = append(down, Sample{Time:current.Time})
		} else if current.Value < previous.Value {
			down = append(down, Sample{Time:current.Time, Value:previous.Value-current.Value})
			up = append(up, Sample{Time:current.Time})
		} else {
			up = append(up, Sample{Time:current.Time})
			down = append(down, Sample{Time:current.Time})
		}
	}
	uSmm := up.EMA(periodCount)
	dSmm := down.EMA(periodCount)
	for i := 0; i < len(dSmm); i++ {
		dSmm[i].Value = math.Max(dSmm[i].Value, epsilon)
		rs := uSmm[i].Value/dSmm[i].Value
		response = append(response, Sample{Value:100. - 100./(1.+rs), Time:dSmm[i].Time})
	}
	return response
}

func (s *Series) Mean() Sample {
	sum := 0.0
	for _, v := range *s {
		sum += v.Value
	}
	return Sample{Value: sum / float64(len(*s)), Time: (*s)[0].Time}
}
