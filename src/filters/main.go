package main

import filter "filters/lib"

func main() {
	filter := filter.NewFilter()
	filter.FilterByDatetimeHour([]string{}, 2023, 2023, 6, 23)
}
