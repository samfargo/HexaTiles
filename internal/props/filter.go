package props

import (
	"path/filepath"
	"sort"
	"strings"
)

// Filter controls which properties are retained on features.
type Filter struct {
	includes     map[string]struct{}
	includeOrder []string
	dropPatterns []string
    keepAllByDefault bool
}

// NewFilter constructs a filter from comma-separated include and drop lists.
// Empty includes means keep all properties by default.
func NewFilter(include []string, drop []string, keepAllByDefault bool) *Filter {
    f := &Filter{keepAllByDefault: keepAllByDefault}
	if len(include) > 0 {
		f.includes = make(map[string]struct{}, len(include))
		for _, item := range include {
			key := strings.TrimSpace(item)
			if key == "" {
				continue
			}
			if _, exists := f.includes[key]; exists {
				continue
			}
			f.includes[key] = struct{}{}
			f.includeOrder = append(f.includeOrder, key)
		}
	}

	for _, pattern := range drop {
		trimmed := strings.TrimSpace(pattern)
		if trimmed == "" {
			continue
		}
		f.dropPatterns = append(f.dropPatterns, trimmed)
	}

	sort.Strings(f.dropPatterns)
	return f
}

// Apply filters the provided property map and returns a new map.
func (f *Filter) Apply(props map[string]any) map[string]any {
	if props == nil {
		return nil
	}
	if len(props) == 0 {
		return map[string]any{}
	}

	filtered := make(map[string]any, len(props))

    if len(f.includes) > 0 {
		for key := range f.includes {
			if value, ok := props[key]; ok && f.shouldKeep(key) {
				filtered[key] = value
			}
		}
		return filtered
	}

    if f.keepAllByDefault {
        for key, value := range props {
            if f.shouldKeep(key) {
                filtered[key] = value
            }
        }
        return filtered
    }

    // Default: keep none (other than system-added fields outside this filter)

	return filtered
}

// Keys returns the list of explicitly included keys, preserving CLI order.
func (f *Filter) Keys() []string {
	return append([]string(nil), f.includeOrder...)
}

func (f *Filter) shouldKeep(key string) bool {
	if len(f.dropPatterns) > 0 {
		for _, pattern := range f.dropPatterns {
			if ok, _ := filepath.Match(pattern, key); ok {
				return false
			}
		}
	}

	if len(f.includes) == 0 {
		return true
	}

	_, ok := f.includes[key]
	return ok
}
