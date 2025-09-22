package props

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Quantizer encapsulates numeric rounding rules for feature properties.
type Quantizer struct {
	FloatStep  float64            // Applied to floating point properties when >0.
	IntStep    float64            // Applied to integer properties when >0.
	FieldSteps map[string]float64 // Optional overrides per property key (case-sensitive).
}

// Result captures quantization statistics for a feature.
type Result struct {
	TotalAbsError float64
	FieldErrors   map[string]float64
	Changes       int
}

// Parse builds a Quantizer from a CLI string such as "float=0.01,int=1,score=0.05".
func Parse(spec string) (Quantizer, error) {
	q := Quantizer{
		FloatStep:  0,
		IntStep:    0,
		FieldSteps: make(map[string]float64),
	}

	if strings.TrimSpace(spec) == "" {
		return q, nil
	}

	tokens := splitSpec(spec)
	for _, token := range tokens {
		if token == "" {
			continue
		}
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			return Quantizer{}, fmt.Errorf("invalid quantize token %q", token)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" || value == "" {
			return Quantizer{}, fmt.Errorf("invalid quantize token %q", token)
		}

		step, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return Quantizer{}, fmt.Errorf("parse quantize value %q: %w", token, err)
		}
		if step < 0 {
			return Quantizer{}, fmt.Errorf("quantize step must be non-negative for %q", token)
		}

		switch strings.ToLower(key) {
		case "float":
			q.FloatStep = step
		case "int":
			q.IntStep = step
		default:
			q.FieldSteps[key] = step
		}
	}

	return q, nil
}

func splitSpec(spec string) []string {
	spec = strings.ReplaceAll(spec, ";", ",")
	spec = strings.ReplaceAll(spec, " ", ",")
	spec = strings.ReplaceAll(spec, "\t", ",")
	fields := strings.Split(spec, ",")
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		trimmed := strings.TrimSpace(f)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// Apply rounds numeric values according to the configured rules, mutating props in place.
func (q Quantizer) Apply(props map[string]any) Result {
	if len(props) == 0 {
		return Result{}
	}

	res := Result{FieldErrors: make(map[string]float64)}

	for key, value := range props {
		if value == nil {
			continue
		}

		step := q.lookupStep(key, value)
		if step <= 0 {
			continue
		}

		switch v := value.(type) {
		case float64:
			quantized, diff, changed := quantizeFloat64(v, step)
			if changed {
				props[key] = quantized
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		case float32:
			quantized, diff, changed := quantizeFloat64(float64(v), step)
			if changed {
				props[key] = float32(quantized)
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		case int64:
			quantized, diff, changed := quantizeInt64(v, step)
			if changed {
				props[key] = quantized
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		case int32:
			quantized, diff, changed := quantizeInt64(int64(v), step)
			if changed {
				props[key] = int32(quantized)
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		case int:
			quantized, diff, changed := quantizeInt64(int64(v), step)
			if changed {
				props[key] = int(quantized)
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		case uint64:
			quantized, diff, changed := quantizeInt64(int64(v), step)
			if changed {
				props[key] = uint64(quantized)
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		case uint32:
			quantized, diff, changed := quantizeInt64(int64(v), step)
			if changed {
				props[key] = uint32(quantized)
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				continue
			}
			quantized, diff, changed := quantizeFloat64(f, step)
			if changed {
				props[key] = quantized
				res.TotalAbsError += diff
				res.FieldErrors[key] += diff
				res.Changes++
			}
		}
	}

	if len(res.FieldErrors) == 0 {
		res.FieldErrors = nil
	}

	return res
}

func (q Quantizer) lookupStep(key string, value any) float64 {
	if q.FieldSteps != nil {
		if step, ok := q.FieldSteps[key]; ok {
			return step
		}
	}

	switch value.(type) {
	case float32, float64:
		return q.FloatStep
	case int, int32, int64, uint32, uint64:
		return q.IntStep
	case json.Number:
		return q.FloatStep
	default:
		return 0
	}
}

func quantizeFloat64(value, step float64) (float64, float64, bool) {
	if step <= 0 {
		return value, 0, false
	}
	quantized := math.Round(value/step) * step
	diff := math.Abs(quantized - value)
	if diff == 0 {
		return value, 0, false
	}
	return quantized, diff, true
}

func quantizeInt64(value int64, step float64) (int64, float64, bool) {
	if step <= 0 {
		return value, 0, false
	}
	quantized := int64(math.Round(float64(value)/step) * step)
	if quantized == value {
		return value, 0, false
	}
	diff := math.Abs(float64(quantized) - float64(value))
	if diff == 0 {
		return value, 0, false
	}
	return quantized, diff, true
}
