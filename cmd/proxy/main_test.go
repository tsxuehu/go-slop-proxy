package main

import (
	"reflect"
	"testing"
)

func TestReorderFlags(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		bools map[string]bool
		want  []string
	}{
		{"bool after positional", []string{"1", "--off"}, offIsBool, []string{"--off", "1"}},
		{"bool before positional", []string{"--off", "1"}, offIsBool, []string{"--off", "1"}},
		{"string flag after positional", []string{"1", "--allow", "alice,bob"}, nil,
			[]string{"--allow", "alice,bob", "1"}},
		{"equals form", []string{"1", "--off=true"}, offIsBool, []string{"--off=true", "1"}},
		{"no flags", []string{"1"}, nil, []string{"1"}},
		{"multiple positionals", []string{"1", "extra", "--off"}, offIsBool,
			[]string{"--off", "1", "extra"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reorderFlags(tc.args, tc.bools)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("reorderFlags(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}
