package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Result struct {
	min   float64
	max   float64
	sum   float64
	count int
}

func main() {
	fo, _ := os.Create("./output.txt")
	w := bufio.NewWriter(fo)
	w.WriteString(process())
}

func process() string {
	resultMap, err := readFile(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	var result []string

	for city, temps := range resultMap {
		avg := temps.sum
		avg = math.Ceil(avg / float64(temps.count))
		result = append(result, fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, temps.min, avg, temps.max))
	}

	sort.Strings(result)
	return strings.Join(result, ", ")
}

func readFile(path string) (resultMap map[string]Result, err error) {
	resultMap = map[string]Result{}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		text := scanner.Text()

		value := strings.Split(text, ";")
		city := value[0]
		num := toFloat(value[1])

		if _, ok := resultMap[city]; ok {
			data := resultMap[city]
			data.count += 1
			data.sum += num
			if data.max < num {
				data.max = num
			}
			if data.min > num {
				data.min = num
			}

			resultMap[city] = data
		} else {
			resultMap[city] = Result{min: num, max: num, sum: num, count: 1}
		}
	}

	return resultMap, nil
}

func toFloat(input string) float64 {
	output, _ := strconv.ParseFloat(input, 64)
	return output
}
