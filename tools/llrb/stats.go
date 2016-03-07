package main

func newgenstats() map[string]int {
	stats := map[string]int{
		"total":     0,
		"get.ok":    0,
		"get.na":    0,
		"min.ok":    0,
		"min.na":    0,
		"max.ok":    0,
		"max.na":    0,
		"delmin.ok": 0,
		"delmin.na": 0,
		"delmax.ok": 0,
		"delmax.na": 0,
		"upsert":    0,
		"insert":    0,
		"delete.ok": 0,
		"delete.na": 0,
		"validate":  0,
		"snapshot":  0,
		"release":   0,
	}
	return stats
}
