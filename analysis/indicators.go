package analysis

import "math"

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
			Time:s[0].Time,
			Close:s[len(s)-1].Value,
			Open:s[0].Value,
			High:max,
			Low:min,
		}
		response = append(response, o)
	}
	return response
}

func(s *Series) MACD(short, long int) Series {
	var response Series
	longSma := s.SMA(long)
	shortSma := s.SMA(short)
	for i := 0; i < len(longSma); i++ {
		response = append(response, Sample{Time:shortSma[i].Time, Value:shortSma[i].Value-longSma[i].Value})
	}
	return response
}

func(s *Series) SMA(periodCount int) Series {
	resp := Series{}
	for i := 0; i < len(*s) - periodCount; i++ {
		slice := (*s)[i:i+periodCount]
		resp = append(resp, (&slice).Mean())
	}
	return resp
}

func (s *Series) EMA(periodCount int) Series {
	percentage := 2./float64(periodCount+1)
	var response Series
	for i := 0; i < len(*s) - periodCount; i++ {
		subSeries := (*s)[i:i+periodCount]
		if i == 0 {
			response = append(response, subSeries.Mean())
			continue
		}
		closingSample := subSeries[len(subSeries)-1]
		response = append(response, Sample{Value:closingSample.Value*percentage+(1-percentage)*response[i-1].Value, Time:subSeries[0].Time})
	}
	return response
}

func (s *Series) Mean() Sample {
	sum := 0.0
	for _, v := range *s {
		sum += v.Value
	}
	return Sample{Value:sum/float64(len(*s)),Time:(*s)[0].Time}
}

